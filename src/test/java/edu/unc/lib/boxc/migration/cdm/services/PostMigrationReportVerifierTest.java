package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.test.BxcEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.SipServiceHelper;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.springframework.http.HttpStatus;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static edu.unc.lib.boxc.migration.cdm.test.PostMigrationReportTestHelper.parseReport;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

/**
 * @author bbpennel
 */
public class PostMigrationReportVerifierTest {
    private static final String BOXC_URL_1 = "http://example.com/bxc/bb3b83d7-2962-4604-a7d0-9afcb4ec99b1";
    private static final String BOXC_URL_2 = "http://example.com/bxc/91c08272-260f-40f1-bb7c-78854d504368";
    private static final String CDM_URL_1 = "http://localhost/cdm/singleitem/collection/proj/id/25";
    private static final String CDM_URL_2 = "http://localhost/cdm/singleitem/collection/proj/id/26";
    private static final String PARENT_COLL_ID = "4fe5080f-41cd-4b1e-9cdd-71203c824cd0";
    private static final String BXC_RECORD_BASE_URL = "https://example.com/bxc/";
    private static final String BXC_API_BASE_URL = "https://example.com/api/";
    private static final String PARENT_COLL_URL = BXC_RECORD_BASE_URL + PARENT_COLL_ID;
    private static final String PARENT_COLL_TITLE = "Latin Studies Program";
    private static final String JSON = "{\"findingAidUrl\":\"https://finding-aids.lib.unc.edu/catalog/40489\"," +
            "\"viewerType\":\"clover\",\"canBulkDownload\":false,\"dataFileUrl\":\"content/6f4b5e38-754f-49ca-a4a0-6441fea95d76\"," +
            "\"markedForDeletion\":false,\"pageSubtitle\":\"TEST.jpg\",\"briefObject\":{\"added\":\"2018-05-24T20:39:18.165Z\"," +
            "\"counts\":{\"child\":1},\"created\":\"2018-05-24T20:39:18.165Z\",\"format\":[\"Image\"],\"parentCollectionName\":\"" + PARENT_COLL_TITLE +"\"," +
            "\"contentStatus\":[\"Not Described\",\"Has Primary Object\"],\"rollup\":\"1a1e9c1a-cdd2-4874-b6cb-8da783919460\"," +
            "\"parentCollectionId\":\"" + PARENT_COLL_ID + "\",\"id\":\"1a1e9c1a-cdd2-4874-b6cb-8da783919460\"," +
            "\"updated\":\"2018-05-25T13:37:01.864Z\",\"fileType\":[\"image/jpeg\"],\"status\":[\"Public Access\"],\"timestamp\":1751312648385}," +
            "\"collectionId\":\"40489\",\"resourceType\":\"Work\"}";
    private AutoCloseable closeable;
    @TempDir
    public Path tmpFolder;
    @Mock
    private CloseableHttpClient httpClient;
    @Mock
    private HttpEntity respEntity;
    private SipServiceHelper testHelper;
    private MigrationProject project;
    private PostMigrationReportService reportGenerator;
    private PostMigrationReportVerifier verifier;

    @BeforeEach
    public void setup() throws Exception {
        closeable = openMocks(this);
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder, "proj", null, "user", CdmEnvironmentHelper.DEFAULT_ENV_ID,
                BxcEnvironmentHelper.DEFAULT_ENV_ID, MigrationProject.PROJECT_SOURCE_CDM);
        testHelper = new SipServiceHelper(project, tmpFolder);
        reportGenerator = new PostMigrationReportService();
        reportGenerator.setProject(project);
        reportGenerator.setChompbConfig(testHelper.getChompbConfig());
        verifier = new PostMigrationReportVerifier();
        verifier.setProject(project);
        verifier.setHttpClient(httpClient);
        verifier.setBxcRecordBaseUrl(BXC_RECORD_BASE_URL);
        verifier.setBxcApiBaseUrl(BXC_API_BASE_URL);
    }

    @AfterEach
    void closeService() throws Exception {
        closeable.close();
    }

    @Test
    public void noReportTest() {
        Assertions.assertThrows(InvalidProjectStateException.class, () -> {
            verifier.verify();
        });
    }

    @Test
    public void reportVerifySuccessTest() throws Exception {
        mockBxcResponses(Map.of(BOXC_URL_1, HttpStatus.OK,
                BOXC_URL_2, HttpStatus.FORBIDDEN), false);

        reportGenerator.init();
        reportGenerator.addRow("25", CDM_URL_1, "Work", BOXC_URL_1, "Redoubt C",
                null, null, null, "", "", 1);
        reportGenerator.addRow("26", CDM_URL_2, "File", BOXC_URL_2, "A file",
                null, null, null, BOXC_URL_1, "Redoubt C", null);
        reportGenerator.closeCsv();

        var outcome = verifier.verify();
        assertEquals(2, outcome.verifiedCount);
        assertEquals(2, outcome.totalRecords);
        assertEquals(0, outcome.urlErrorCount);
        assertEquals(0, outcome.parentCollErrorCount);

        var rows = parseReport(project);
        assertRowContainsAllInfo(rows, "25",
                CDM_URL_1,
                "Work",
                BOXC_URL_1,
                "Redoubt C",
                "",
                "",
                HttpStatus.OK.name(),
                "",
                "",
                "1",
                PARENT_COLL_URL,
                PARENT_COLL_TITLE);
        assertRowContainsAllInfo(rows, "26",
                CDM_URL_2,
                "File",
                BOXC_URL_2,
                "A file",
                "",
                "",
                HttpStatus.OK.name(),
                BOXC_URL_1,
                "Redoubt C",
                "",
                PARENT_COLL_URL,
                PARENT_COLL_TITLE);
    }

    @Test
    public void reportVerifyErrorsTest() throws Exception {
        mockBxcResponses(Map.of(BOXC_URL_1, HttpStatus.NOT_FOUND,
                BOXC_URL_2, HttpStatus.NOT_FOUND), false);

        reportGenerator.init();
        reportGenerator.addRow("25", CDM_URL_1, "Work", BOXC_URL_1, "Redoubt C",
                null, null, null, "", "", 1);
        reportGenerator.addRow("26", CDM_URL_2, "File", BOXC_URL_2, "A file",
                null, null, null, BOXC_URL_1, "Redoubt C", null);
        reportGenerator.closeCsv();

        var outcome = verifier.verify();
        assertEquals(2, outcome.verifiedCount);
        assertEquals(2, outcome.totalRecords);
        assertEquals(2, outcome.urlErrorCount);
        assertEquals(0, outcome.parentCollErrorCount);

        var rows = parseReport(project);
        assertRowContainsAllInfo(rows, "25",
                CDM_URL_1,
                "Work",
                BOXC_URL_1,
                "Redoubt C",
                "",
                "",
                HttpStatus.NOT_FOUND.name(),
                "",
                "",
                "1",
                PARENT_COLL_URL,
                PARENT_COLL_TITLE);
        assertRowContainsAllInfo(rows, "26",
                CDM_URL_2,
                "File",
                BOXC_URL_2,
                "A file",
                "",
                "",
                HttpStatus.NOT_FOUND.name(),
                BOXC_URL_1,
                "Redoubt C",
                "",
                PARENT_COLL_URL,
                PARENT_COLL_TITLE);
    }

    @Test
    public void reportVerifyPartialTest() throws Exception {
        mockBxcResponses(Map.of(BOXC_URL_2, HttpStatus.OK), false);

        reportGenerator.init();
        reportGenerator.addRow("25", CDM_URL_1, "Work", BOXC_URL_1, "Redoubt C",
                null, null, HttpStatus.OK.name(), "", "", 1);
        reportGenerator.addRow("26", CDM_URL_2, "File", BOXC_URL_2, "A file",
                null, null, null, BOXC_URL_1, "Redoubt C", null);
        reportGenerator.closeCsv();

        var outcome = verifier.verify();
        assertEquals(1, outcome.verifiedCount);
        assertEquals(2, outcome.totalRecords);
        assertEquals(0, outcome.urlErrorCount);
        assertEquals(0, outcome.parentCollErrorCount);

        var rows = parseReport(project);
        assertRowContainsAllInfo(rows, "25",
                CDM_URL_1,
                "Work",
                BOXC_URL_1,
                "Redoubt C",
                "",
                "",
                HttpStatus.OK.name(),
                "",
                "",
                "1",
                PARENT_COLL_URL,
                PARENT_COLL_TITLE);
        assertRowContainsAllInfo(rows, "26",
                CDM_URL_2,
                "File",
                BOXC_URL_2,
                "A file",
                "",
                "",
                HttpStatus.OK.name(),
                BOXC_URL_1,
                "Redoubt C",
                "",
                PARENT_COLL_URL,
                PARENT_COLL_TITLE);
    }

    @Test
    public void reportVerifyWithParentCollectionError() throws Exception {
        mockBxcResponses(Map.of(BOXC_URL_1, HttpStatus.OK, BOXC_URL_2, HttpStatus.OK), true);

        reportGenerator.init();
        reportGenerator.addRow("25", CDM_URL_1, "Work", BOXC_URL_1, "Redoubt C",
                null, null, null, "", "", 1);
        reportGenerator.addRow("26", CDM_URL_2, "File", BOXC_URL_2, "A file",
                null, null, null, BOXC_URL_1, "Redoubt C", null);
        reportGenerator.closeCsv();

        var outcome = verifier.verify();
        assertEquals(2, outcome.verifiedCount);
        assertEquals(2, outcome.totalRecords);
        assertEquals(0, outcome.urlErrorCount);
        assertEquals(2, outcome.parentCollErrorCount);

        var rows = parseReport(project);
        assertRowContainsAllInfo(rows, "25",
                CDM_URL_1,
                "Work",
                BOXC_URL_1,
                "Redoubt C",
                "",
                "",
                HttpStatus.OK.name(),
                "",
                "",
                "1",
                "",
                "");
        assertRowContainsAllInfo(rows, "26",
                CDM_URL_2,
                "File",
                BOXC_URL_2,
                "A file",
                "",
                "",
                HttpStatus.OK.name(),
                BOXC_URL_1,
                "Redoubt C",
                "",
                "",
                "");
    }

    private void mockBxcResponses(Map<String, HttpStatus> urlToStatus, boolean apiFailure) throws IOException {
        when(httpClient.execute(any(HttpUriRequest.class))).thenAnswer(invocation -> {
            HttpGet httpGet = invocation.getArgument(0);
            var requestUrl = httpGet.getURI().toString();
            var resp = mock(CloseableHttpResponse.class);
            when(resp.getEntity()).thenReturn(respEntity);
            var statusLine = mock(StatusLine.class);
            when(resp.getStatusLine()).thenReturn(statusLine);

            if (requestUrl.contains(BXC_API_BASE_URL)) {
                if (apiFailure) {
                    when(statusLine.getStatusCode()).thenReturn(HttpStatus.INTERNAL_SERVER_ERROR.value());
                    return resp;
                }
                when(statusLine.getStatusCode()).thenReturn(HttpStatus.OK.value());
                when(respEntity.getContent()).thenReturn(new ByteArrayInputStream(JSON.getBytes(StandardCharsets.UTF_8)));
            } else {
                when(statusLine.getStatusCode()).thenReturn(urlToStatus.get(httpGet.getURI().toString()).value());
            }
            return resp;
        });
    }

    public static void assertRowContainsAllInfo(List<List<String>> rows, String cdmId, String cdmUrl, String objType,
                                         String bxcUrl, String bxcTitle, String matchingValue, String sourceFile,
                                         String verified, String parentUrl, String parentTitle, String childCount,
                                         String parentCollUrl, String parentCollTitle) {
        var found = rows.stream().filter(r -> r.getFirst().equals(cdmId)).findFirst().orElse(null);
        assertNotNull(found, "Did not find row for CDM id " + cdmId + ", rows were:\n" + rows);
        assertEquals(Arrays.asList(cdmId, cdmUrl, objType, bxcUrl, bxcTitle, matchingValue, sourceFile, verified,
                parentUrl, parentTitle, childCount, parentCollUrl, parentCollTitle), found);
    }
}
