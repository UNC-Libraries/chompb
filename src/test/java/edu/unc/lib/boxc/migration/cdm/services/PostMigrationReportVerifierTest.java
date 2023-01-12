package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.test.BxcEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.SipServiceHelper;
import edu.unc.lib.boxc.migration.cdm.util.PostMigrationReportConstants;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.springframework.http.HttpStatus;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import static edu.unc.lib.boxc.migration.cdm.test.PostMigrationReportTestHelper.assertContainsRow;
import static edu.unc.lib.boxc.migration.cdm.test.PostMigrationReportTestHelper.parseReport;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

/**
 * @author bbpennel
 */
public class PostMigrationReportVerifierTest {
    private static final String BOXC_URL_1 = "http://example.com/bxc/bb3b83d7-2962-4604-a7d0-9afcb4ec99b1";
    private static final String BOXC_URL_2 = "http://example.com/bxc/91c08272-260f-40f1-bb7c-78854d504368";
    private static final String CDM_URL_1 = "http://localhost/cdm/singleitem/collection/proj/id/25";
    private static final String CDM_URL_2 = "http://localhost/cdm/singleitem/collection/proj/id/26";
    @TempDir
    public Path tmpFolder;
    @Mock
    private CloseableHttpClient httpClient;
    private SipServiceHelper testHelper;
    private MigrationProject project;
    private PostMigrationReportService reportGenerator;
    private PostMigrationReportVerifier verifier;

    @BeforeEach
    public void setup() throws Exception {
        initMocks(this);
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder, "proj", null, "user",
                CdmEnvironmentHelper.DEFAULT_ENV_ID, BxcEnvironmentHelper.DEFAULT_ENV_ID);
        testHelper = new SipServiceHelper(project, tmpFolder);
        reportGenerator = new PostMigrationReportService();
        reportGenerator.setProject(project);
        reportGenerator.setChompbConfig(testHelper.getChompbConfig());
        verifier = new PostMigrationReportVerifier();
        verifier.setProject(project);
        verifier.setHttpClient(httpClient);
    }

    @Test
    public void noReportTest() throws Exception {
        Assertions.assertThrows(InvalidProjectStateException.class, () -> {
            verifier.verify();
        });
    }

    @Test
    public void reportVerifySuccessTest() throws Exception {
        mockBxcResponses(Map.of(BOXC_URL_1, HttpStatus.OK,
                BOXC_URL_2, HttpStatus.FORBIDDEN));

        reportGenerator.init();
        reportGenerator.addRow("25", CDM_URL_1, "Work", BOXC_URL_1, "Redoubt C",
                null, "", "", 1);
        reportGenerator.addRow("26", CDM_URL_2, "File", BOXC_URL_2, "A file",
        null, BOXC_URL_1, "Redoubt C", null);
        reportGenerator.closeCsv();

        var outcome = verifier.verify();
        assertEquals(2, outcome.verifiedCount);
        assertEquals(2, outcome.totalRecords);
        assertEquals(0, outcome.errorCount);

        var rows = parseReport(project);
        assertContainsRow(rows, "25",
                CDM_URL_1,
                "Work",
                BOXC_URL_1,
                "Redoubt C",
                HttpStatus.OK.name(),
                "",
                "",
                "1");
        assertContainsRow(rows, "26",
                CDM_URL_2,
                "File",
                BOXC_URL_2,
                "A file",
                HttpStatus.OK.name(),
                BOXC_URL_1,
                "Redoubt C",
                "");
    }

    @Test
    public void reportVerifyErrorsTest() throws Exception {
        mockBxcResponses(Map.of(BOXC_URL_1, HttpStatus.NOT_FOUND,
                BOXC_URL_2, HttpStatus.NOT_FOUND));

        reportGenerator.init();
        reportGenerator.addRow("25", CDM_URL_1, "Work", BOXC_URL_1, "Redoubt C",
                null, "", "", 1);
        reportGenerator.addRow("26", CDM_URL_2, "File", BOXC_URL_2, "A file",
                null, BOXC_URL_1, "Redoubt C", null);
        reportGenerator.closeCsv();

        var outcome = verifier.verify();
        assertEquals(2, outcome.verifiedCount);
        assertEquals(2, outcome.totalRecords);
        assertEquals(2, outcome.errorCount);

        var rows = parseReport(project);
        assertContainsRow(rows, "25",
                CDM_URL_1,
                "Work",
                BOXC_URL_1,
                "Redoubt C",
                HttpStatus.NOT_FOUND.name(),
                "",
                "",
                "1");
        assertContainsRow(rows, "26",
                CDM_URL_2,
                "File",
                BOXC_URL_2,
                "A file",
                HttpStatus.NOT_FOUND.name(),
                BOXC_URL_1,
                "Redoubt C",
                "");
    }

    @Test
    public void reportVerifyPartialTest() throws Exception {
        mockBxcResponses(Map.of(BOXC_URL_2, HttpStatus.OK));

        reportGenerator.init();
        reportGenerator.addRow("25", CDM_URL_1, "Work", BOXC_URL_1, "Redoubt C",
                HttpStatus.OK.name(), "", "", 1);
        reportGenerator.addRow("26", CDM_URL_2, "File", BOXC_URL_2, "A file",
                null, BOXC_URL_1, "Redoubt C", null);
        reportGenerator.closeCsv();

        var outcome = verifier.verify();
        assertEquals(1, outcome.verifiedCount);
        assertEquals(2, outcome.totalRecords);
        assertEquals(0, outcome.errorCount);

        var rows = parseReport(project);
        assertContainsRow(rows, "25",
                CDM_URL_1,
                "Work",
                BOXC_URL_1,
                "Redoubt C",
                HttpStatus.OK.name(),
                "",
                "",
                "1");
        assertContainsRow(rows, "26",
                CDM_URL_2,
                "File",
                BOXC_URL_2,
                "A file",
                HttpStatus.OK.name(),
                BOXC_URL_1,
                "Redoubt C",
                "");
    }

    private void mockBxcResponses(Map<String, HttpStatus> urlToStatus) throws IOException {
        when(httpClient.execute(any(HttpUriRequest.class))).thenAnswer(invocation -> {
            HttpGet httpGet = invocation.getArgument(0);
            var resp1 = mock(CloseableHttpResponse.class);
            var statusLine1 = mock(StatusLine.class);
            when(resp1.getStatusLine()).thenReturn(statusLine1);
            when(statusLine1.getStatusCode()).thenReturn(urlToStatus.get(httpGet.getURI().toString()).value());
            return resp1;
        });
    }
}
