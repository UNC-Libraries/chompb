/**
 * Copyright 2008 The University of North Carolina at Chapel Hill
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
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
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mock;
import org.springframework.http.HttpStatus;

import java.io.IOException;
import java.util.Map;

import static edu.unc.lib.boxc.migration.cdm.test.PostMigrationReportTestHelper.assertContainsRow;
import static edu.unc.lib.boxc.migration.cdm.test.PostMigrationReportTestHelper.parseReport;
import static org.junit.Assert.assertEquals;
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
    @Rule
    public final TemporaryFolder tmpFolder = new TemporaryFolder();
    @Mock
    private CloseableHttpClient httpClient;
    private SipServiceHelper testHelper;
    private MigrationProject project;
    private PostMigrationReportService reportGenerator;
    private PostMigrationReportVerifier verifier;

    @Before
    public void setup() throws Exception {
        initMocks(this);
        tmpFolder.create();
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder.getRoot().toPath(), "proj", null, "user",
                CdmEnvironmentHelper.DEFAULT_ENV_ID, BxcEnvironmentHelper.DEFAULT_ENV_ID);
        testHelper = new SipServiceHelper(project, tmpFolder.newFolder().toPath());
        reportGenerator = new PostMigrationReportService();
        reportGenerator.setProject(project);
        reportGenerator.setChompbConfig(testHelper.getChompbConfig());
        verifier = new PostMigrationReportVerifier();
        verifier.setProject(project);
        verifier.setHttpClient(httpClient);
    }

    @Test(expected = InvalidProjectStateException.class)
    public void noReportTest() throws Exception {
        verifier.verify();
    }

    @Test
    public void reportVerifySuccessTest() throws Exception {
        mockBxcResponses(Map.of(BOXC_URL_1, HttpStatus.OK,
                BOXC_URL_2, HttpStatus.OK));

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
                PostMigrationReportConstants.VERIFIED_OK,
                "",
                "",
                "1");
        assertContainsRow(rows, "26",
                CDM_URL_2,
                "File",
                BOXC_URL_2,
                "A file",
                PostMigrationReportConstants.VERIFIED_OK,
                BOXC_URL_1,
                "Redoubt C",
                "");
    }

    @Test
    public void reportVerifyErrorsTest() throws Exception {
        mockBxcResponses(Map.of(BOXC_URL_1, HttpStatus.FORBIDDEN,
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
                HttpStatus.FORBIDDEN.name(),
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
                PostMigrationReportConstants.VERIFIED_OK, "", "", 1);
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
                PostMigrationReportConstants.VERIFIED_OK,
                "",
                "",
                "1");
        assertContainsRow(rows, "26",
                CDM_URL_2,
                "File",
                BOXC_URL_2,
                "A file",
                PostMigrationReportConstants.VERIFIED_OK,
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
