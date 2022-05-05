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

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import edu.unc.lib.boxc.migration.cdm.options.CdmExportOptions;
import edu.unc.lib.boxc.migration.cdm.services.export.ExportStateService;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo.CdmFieldEntry;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * @author bbpennel
 */
public class CdmExportServiceTest {
    private static final String CDM_BASE_URL = "http://example.com:88/";
    private static final String PROJECT_NAME = "proj";
    private static final String EXPORT_BODY = "<xml><rec>1</rec></xml>";
    @Rule
    public final TemporaryFolder tmpFolder = new TemporaryFolder();

    private MigrationProject project;
    private CdmFieldService fieldService;
    private CdmExportService service;
    private ExportStateService exportStateService;
    @Mock
    private CdmFileRetrievalService cdmFileRetrievalService;
    @Mock
    private CloseableHttpClient httpClient;
    @Mock
    private CloseableHttpResponse getResp;
    @Mock
    private StatusLine getStatus;
    @Mock
    private CloseableHttpResponse postResp;
    @Mock
    private StatusLine postStatus;
    @Mock
    private HttpEntity postEntity;
    @Mock
    private HttpEntity getEntity;
    @Captor
    private ArgumentCaptor<HttpUriRequest> requestCaptor;
    private String descAllPath = "src/test/resources/descriptions/gilmer/index/description/desc.all";

    @Before
    public void setup() throws Exception {
        initMocks(this);
        tmpFolder.create();
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder.getRoot().toPath(), PROJECT_NAME, null, "user");
        fieldService = new CdmFieldService();
        exportStateService = new ExportStateService();
        exportStateService.setProject(project);
        exportStateService.transitionToStarting();
        service = new CdmExportService();
        service.setHttpClient(httpClient);
        service.setProject(project);
        service.setCdmFieldService(fieldService);
        service.setExportStateService(exportStateService);
        service.setFileRetrievalService(cdmFileRetrievalService);

        // Trigger population of desc.all file
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                FileUtils.copyFile(new File(descAllPath),
                        project.getExportPath().resolve(CdmFileRetrievalService.DESC_ALL_FILENAME).toFile());
                return null;
            }
        }).when(cdmFileRetrievalService).downloadDescAllFile();

        // Mockito any matcher not differentiating between the different HttpPost/HttpGet params
        when(httpClient.execute(any())).thenReturn(getResp).thenReturn(getResp).thenReturn(postResp).thenReturn(getResp);
        when(getResp.getStatusLine()).thenReturn(getStatus);
        when(getResp.getEntity()).thenReturn(getEntity);
        when(postResp.getStatusLine()).thenReturn(postStatus);
        when(postResp.getEntity()).thenReturn(postEntity);
        when(getEntity.getContent()).thenReturn(new ByteArrayInputStream(EXPORT_BODY.getBytes()));
    }

    private CdmExportOptions makeExportOptions() {
        CdmExportOptions options = new CdmExportOptions();
        options.setCdmBaseUri(CDM_BASE_URL);
        options.setCdmUsername("user");
        options.setPageSize(1000);
        return options;
    }

    @Test
    public void exportAllValidProjectTest() throws Exception {
        CdmFieldInfo fieldInfo = populateFieldInfo();
        fieldService.persistFieldsToProject(project, fieldInfo);

        when(postStatus.getStatusCode()).thenReturn(200);
        when(getStatus.getStatusCode()).thenReturn(200);
        when(getEntity.getContent())
                .thenReturn(this.getClass().getResourceAsStream("/sample_exports/gilmer/export_all.xml"));

        service.exportAll(makeExportOptions());

        verify(httpClient, times(2)).execute(requestCaptor.capture());
        List<HttpUriRequest> requests = requestCaptor.getAllValues();
        HttpPost httpPost = (HttpPost) requests.get(0);
        String postBody = IOUtils.toString(httpPost.getEntity().getContent(), ISO_8859_1);
        Map<String, String> submittedParams = Arrays.stream(postBody.split("&"))
            .collect(Collectors.toMap(f -> f.split("=")[0], f -> f.split("=")[1]));
        assertEquals("1", submittedParams.get("CISOPAGE"));
        assertEquals("standard", submittedParams.get("CISOTYPE"));
        assertEquals("%2F" + PROJECT_NAME, submittedParams.get("CISODB"));
        assertEquals("title", submittedParams.get("title"));
        assertEquals("description", submittedParams.get("desc"));

        assertTrue("Export folder not created", Files.exists(project.getExportPath()));
        assertExportFileCount(1);
        assertEquals(IOUtils.toString(getClass().getResourceAsStream("/sample_exports/gilmer/export_all.xml"), StandardCharsets.UTF_8), FileUtils.readFileToString(
                project.getExportPath().resolve("export_1.xml").toFile(), StandardCharsets.UTF_8));
    }

    @Test
    public void exportAllExportFailureTest() throws Exception {
        CdmFieldInfo fieldInfo = populateFieldInfo();
        fieldService.persistFieldsToProject(project, fieldInfo);

        when(postStatus.getStatusCode()).thenReturn(400);
        when(postEntity.getContent()).thenReturn(new ByteArrayInputStream("bad".getBytes()));
        when(getStatus.getStatusCode()).thenReturn(200);
        when(getEntity.getContent())
                .thenReturn(new ByteArrayInputStream("bad".getBytes()))
                .thenReturn(new ByteArrayInputStream("bad".getBytes()));

        try {
            service.exportAll(makeExportOptions());
            fail();
        } catch (MigrationException e) {
            // Should only have made one call
            verify(httpClient, times(2)).execute(any());
            // only desc.all file should be present
            assertExportFileCount(1);
        }
    }

    @Test
    public void exportAllDownloadFailureTest() throws Exception {
        CdmFieldInfo fieldInfo = populateFieldInfo();
        fieldService.persistFieldsToProject(project, fieldInfo);

        when(postStatus.getStatusCode()).thenReturn(200);
        // First status is for object listing requests
        when(getStatus.getStatusCode()).thenReturn(200, 200, 400);
        when(getEntity.getContent())
                .thenReturn(new ByteArrayInputStream("bad".getBytes()));

        try {
            service.exportAll(makeExportOptions());
            fail();
        } catch (MigrationException e) {
            verify(httpClient, times(2)).execute(any());
            assertExportFileCount(1);
        }
    }

    @Test
    public void exportAllSkipFieldTest() throws Exception {
        CdmFieldInfo fieldInfo = populateFieldInfo();
        fieldInfo.getFields().get(1).setSkipExport(true);
        fieldService.persistFieldsToProject(project, fieldInfo);

        when(postStatus.getStatusCode()).thenReturn(200);
        when(getStatus.getStatusCode()).thenReturn(200);
        when(getEntity.getContent())
                .thenReturn(this.getClass().getResourceAsStream("/sample_exports/gilmer/export_all.xml"));

        service.exportAll(makeExportOptions());

        verify(httpClient, times(2)).execute(requestCaptor.capture());
        List<HttpUriRequest> requests = requestCaptor.getAllValues();
        HttpPost httpPost = (HttpPost) requests.get(0);
        String postBody = IOUtils.toString(httpPost.getEntity().getContent(), ISO_8859_1);
        Map<String, String> submittedParams = Arrays.stream(postBody.split("&"))
            .collect(Collectors.toMap(f -> f.split("=")[0], f -> f.split("=")[1]));
        assertEquals("1", submittedParams.get("CISOPAGE"));
        assertEquals("standard", submittedParams.get("CISOTYPE"));
        assertEquals("%2F" + PROJECT_NAME, submittedParams.get("CISODB"));
        assertEquals("title", submittedParams.get("title"));
        assertFalse("Must not include skipped field", submittedParams.containsKey("desc"));

        assertTrue("Export folder not created", Files.exists(project.getExportPath()));
        assertExportFileCount(1);
        assertEquals(IOUtils.toString(getClass().getResourceAsStream("/sample_exports/gilmer/export_all.xml"), StandardCharsets.UTF_8), FileUtils.readFileToString(
                project.getExportPath().resolve("export_1.xml").toFile(), StandardCharsets.UTF_8));
    }

    @Test
    public void exportPageMissingIdFromExportTest() throws Exception {
        CdmFieldInfo fieldInfo = populateFieldInfo();
        fieldService.persistFieldsToProject(project, fieldInfo);

        var pageIds = Arrays.asList("216", "604", "607", "666");
        exportStateService.startOrResumeExport(false);
        // Include an extra id in the list of ids that isn't present in the export
        exportStateService.transitionToDownloadingDesc();
        exportStateService.transitionToListing();
        exportStateService.transitionToExporting(4, 10);

        when(postStatus.getStatusCode()).thenReturn(200);
        when(getStatus.getStatusCode()).thenReturn(200);
        when(getEntity.getContent())
                .thenReturn(this.getClass().getResourceAsStream("/sample_exports/export_compounds.xml"));

        try {
            service.exportPageOfResults(0, pageIds, "", makeExportOptions());
            fail();
        } catch (MigrationException e) {
            assertTrue(e.toString(), e.getMessage()
                    .contains("document returned by CDM was missing the following records"));
            assertTrue(e.toString(), e.getMessage().contains("666"));
        }
    }

    @Test
    public void exportPageExtraIdFromExportTest() throws Exception {
        CdmFieldInfo fieldInfo = populateFieldInfo();
        fieldService.persistFieldsToProject(project, fieldInfo);

        var pageIds = Arrays.asList("604", "607");
        exportStateService.startOrResumeExport(false);
        // Include an extra id in the list of ids that isn't present in the export
        exportStateService.transitionToDownloadingDesc();
        exportStateService.transitionToListing();
        exportStateService.transitionToExporting(2, 10);

        when(postStatus.getStatusCode()).thenReturn(200);
        when(getStatus.getStatusCode()).thenReturn(200);
        when(getEntity.getContent())
                .thenReturn(this.getClass().getResourceAsStream("/sample_exports/export_compounds.xml"));

        try {
            service.exportPageOfResults(0, pageIds, "", makeExportOptions());
            fail();
        } catch (MigrationException e) {
            assertTrue(e.toString(), e.getMessage()
                    .contains("document contained IDs not in the requested set"));
            assertTrue(e.toString(), e.getMessage().contains("216"));
        }
    }

    private void assertExportFileCount(int expectedCount) throws Exception {
        try (Stream<Path> files = Files.list(project.getExportPath())) {
            long count = files.filter(p -> p.getFileName().toString().startsWith("export")).count();
            assertEquals(expectedCount, count);
        }
    }

    private CdmFieldInfo populateFieldInfo() {
        CdmFieldInfo fieldInfo = new CdmFieldInfo();
        List<CdmFieldEntry> fields = fieldInfo.getFields();
        CdmFieldEntry field1 = new CdmFieldEntry();
        field1.setNickName("title");
        field1.setExportAs("title");
        field1.setDescription("Title");
        fields.add(field1);
        CdmFieldEntry field2 = new CdmFieldEntry();
        field2.setNickName("desc");
        field2.setExportAs("description");
        field2.setDescription("Abstract");
        fields.add(field2);
        return fieldInfo;
    }
}
