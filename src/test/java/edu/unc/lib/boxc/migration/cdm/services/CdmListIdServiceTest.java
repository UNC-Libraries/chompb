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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.export.ExportStateService;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

public class CdmListIdServiceTest {
    private static final String CDM_BASE_URL = "http://example.com:88/";
    private static final String PROJECT_NAME = "gilmer";
    public static final String CDM_QUERY_BASE = CDM_BASE_URL + CdmListIdService.CDM_QUERY_BASE
            + PROJECT_NAME + "/0/dmrecord/dmrecord/";
    @Rule
    public final TemporaryFolder tmpFolder = new TemporaryFolder();

    private MigrationProject project;
    private CdmListIdService service;
    private ExportStateService exportStateService;
    @Mock
    private CloseableHttpClient httpClient;
    @Mock
    private CloseableHttpResponse httpResp;
    @Mock
    private HttpEntity respEntity;
    @Mock
    private StatusLine statusLine;
    @Captor
    private ArgumentCaptor<HttpGet> httpGetArgumentCaptor;

    @Before
    public void setup() throws Exception {
        initMocks(this);
        tmpFolder.create();
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder.getRoot().toPath(), PROJECT_NAME, null, "user");
        Files.createDirectories(project.getExportPath());
        exportStateService = new ExportStateService();
        exportStateService.setProject(project);
        exportStateService.transitionToStarting();
        service = new CdmListIdService();
        service.setHttpClient(httpClient);
        service.setCdmBaseUri(CDM_BASE_URL);
        service.setExportStateService(exportStateService);
        service.setPageSize(50);
        service.setProject(project);

        when(httpClient.execute(any(HttpGet.class))).thenReturn(httpResp);
        when(httpResp.getEntity()).thenReturn(respEntity);
        when(httpResp.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(200);
    }

    @Test
    public void retrieveCdmListIdTest() throws Exception {
        when(respEntity.getContent()).thenReturn(this.getClass().getResourceAsStream("/sample_pages/cdm_listid_resp.json"))
                .thenReturn(this.getClass().getResourceAsStream("/sample_pages/page_1.json"))
                .thenReturn(this.getClass().getResourceAsStream("/sample_pages/page_2.json"))
                .thenReturn(this.getClass().getResourceAsStream("/sample_pages/page_3.json"))
                .thenReturn(this.getClass().getResourceAsStream("/sample_pages/page_4.json"));

        List<String> allListId = service.listAllCdmIds();

        assertEquals(161, allListId.size());
        assertEquals("25", allListId.get(0));
        assertEquals("130", allListId.get(99));
        assertEquals("131", allListId.get(100));
        assertEquals("193", allListId.get(160));

        assertUrlsCalled(CDM_QUERY_BASE + "1/0/1/0/0/0/0/json",
                CDM_QUERY_BASE + "50/1/1/0/0/0/0/json",
                CDM_QUERY_BASE + "50/51/1/0/0/0/0/json",
                CDM_QUERY_BASE + "50/101/1/0/0/0/0/json",
                CDM_QUERY_BASE + "50/151/1/0/0/0/0/json");
    }

    @Test
    public void retrieveCdmListIdNoResults() throws Exception {
        when(respEntity.getContent()).thenReturn(this.getClass().getResourceAsStream("/sample_pages/cdm_listid_resp.json"));
        try {
            service.listAllCdmIds();
            fail();
        } catch (MigrationException e) {
            assertTrue(e.getMessage().contains("Retrieved no object ids"));
        }
    }

    @Test
    public void retrieveCdmListIdLessThanPageSize() throws Exception {
        service.setPageSize(1000);
        when(respEntity.getContent()).thenReturn(this.getClass().getResourceAsStream("/sample_pages/cdm_listid_resp.json"))
                .thenReturn(this.getClass().getResourceAsStream("/sample_pages/page_all.json"));

        List<String> allListId = service.listAllCdmIds();

        assertEquals(161, allListId.size());
        assertEquals("25", allListId.get(0));
        assertEquals("130", allListId.get(99));
        assertEquals("131", allListId.get(100));
        assertEquals("193", allListId.get(160));

        assertUrlsCalled(CDM_QUERY_BASE + "1/0/1/0/0/0/0/json",
                CDM_QUERY_BASE + "1000/1/1/0/0/0/0/json");
    }

    @Test
    public void resumeRetrieveCdmListIdTest() throws Exception {
        when(respEntity.getContent()).thenReturn(this.getClass().getResourceAsStream("/sample_pages/cdm_listid_resp.json"))
                .thenReturn(this.getClass().getResourceAsStream("/sample_pages/page_1.json"))
                .thenReturn(this.getClass().getResourceAsStream("/sample_pages/page_2.json"))
                .thenThrow(new ConnectTimeoutException("Boom"));

        try {
            service.listAllCdmIds();
            fail();
        } catch (ConnectTimeoutException e) {
        }

        // Verify that the first 2 pages of results were captured
        List<String> firstPassIds = exportStateService.retrieveObjectIds();
        assertEquals(100, firstPassIds.size());
        assertEquals("25", firstPassIds.get(0));
        assertEquals("74", firstPassIds.get(49));
        assertEquals("130", firstPassIds.get(99));
        assertEquals(161, exportStateService.getState().getTotalObjects().intValue());

        reset(respEntity);
        when(respEntity.getContent()).thenReturn(this.getClass().getResourceAsStream("/sample_pages/page_3.json"))
                .thenReturn(this.getClass().getResourceAsStream("/sample_pages/page_4.json"));

        // Run it again with resume
        exportStateService.getState().setResuming(true);
        List<String> allListId = service.listAllCdmIds();

        assertEquals(161, allListId.size());
        assertEquals("25", allListId.get(0));
        assertEquals("130", allListId.get(99));
        assertEquals("131", allListId.get(100));
        assertEquals("193", allListId.get(160));

        assertUrlsCalled(CDM_QUERY_BASE + "1/0/1/0/0/0/0/json",
                CDM_QUERY_BASE + "50/1/1/0/0/0/0/json",
                CDM_QUERY_BASE + "50/51/1/0/0/0/0/json",
                // Page called twice, first time fails
                CDM_QUERY_BASE + "50/101/1/0/0/0/0/json",
                CDM_QUERY_BASE + "50/101/1/0/0/0/0/json",
                CDM_QUERY_BASE + "50/151/1/0/0/0/0/json");
    }

    @Test
    public void failOnFirstPageOfResultsTest() throws Exception {
        when(respEntity.getContent()).thenReturn(this.getClass().getResourceAsStream("/sample_pages/cdm_listid_resp.json"))
                .thenReturn(new ByteArrayInputStream("Bad output".getBytes(StandardCharsets.UTF_8)))
                .thenReturn(this.getClass().getResourceAsStream("/sample_pages/page_1.json"))
                .thenReturn(this.getClass().getResourceAsStream("/sample_pages/page_2.json"))
                .thenReturn(this.getClass().getResourceAsStream("/sample_pages/page_3.json"))
                .thenReturn(this.getClass().getResourceAsStream("/sample_pages/page_4.json"));

        try {
            service.listAllCdmIds();
            fail();
        } catch (MigrationException e) {
            // Expected
        }

        // Resume with issue resolved
        exportStateService.getState().setResuming(true);
        List<String> allListId = service.listAllCdmIds();

        assertEquals(161, allListId.size());
        assertEquals("25", allListId.get(0));
        assertEquals("130", allListId.get(99));
        assertEquals("131", allListId.get(100));
        assertEquals("193", allListId.get(160));

        assertUrlsCalled(CDM_QUERY_BASE + "1/0/1/0/0/0/0/json",
                CDM_QUERY_BASE + "50/1/1/0/0/0/0/json",
                CDM_QUERY_BASE + "50/1/1/0/0/0/0/json",
                CDM_QUERY_BASE + "50/51/1/0/0/0/0/json",
                CDM_QUERY_BASE + "50/101/1/0/0/0/0/json",
                CDM_QUERY_BASE + "50/151/1/0/0/0/0/json");
    }

    private void assertUrlsCalled(String... expectedUrls) throws Exception {
        verify(httpClient, Mockito.times(expectedUrls.length)).execute(httpGetArgumentCaptor.capture());
        List<HttpGet> getObjects = httpGetArgumentCaptor.getAllValues();
        for (int i = 0; i < expectedUrls.length; i++) {
            HttpGet getMethod = getObjects.get(i);
            String expectedUrl = expectedUrls[i];
            assertEquals(expectedUrl, getMethod.getURI().toString());
        }
    }
}
