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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
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

    @Before
    public void setup() throws Exception {
        initMocks(this);
        tmpFolder.create();
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder.getRoot().toPath(), PROJECT_NAME, null, "user");
        fieldService = new CdmFieldService();

        service = new CdmExportService();
        service.setHttpClient(httpClient);
        service.setCdmBaseUri(CDM_BASE_URL);
        service.setCdmFieldService(fieldService);

        // Mockito any matcher not differentiating between the different HttpPost/HttpGet params
        when(httpClient.execute(any())).thenReturn(postResp).thenReturn(getResp);
        when(getResp.getStatusLine()).thenReturn(getStatus);
        when(postResp.getStatusLine()).thenReturn(postStatus);
        when(postResp.getEntity()).thenReturn(postEntity);
        when(getResp.getEntity()).thenReturn(getEntity);
        when(getEntity.getContent()).thenReturn(new ByteArrayInputStream(EXPORT_BODY.getBytes()));
    }

    @Test
    public void exportAllValidProjectTest() throws Exception {
        CdmFieldInfo fieldInfo = populateFieldInfo();
        fieldService.persistFieldsToProject(project, fieldInfo);

        when(postStatus.getStatusCode()).thenReturn(200);
        when(getStatus.getStatusCode()).thenReturn(200);

        service.exportAll(project);

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
        assertEquals(EXPORT_BODY, FileUtils.readFileToString(
                project.getExportPath().resolve("export_all.xml").toFile(), ISO_8859_1));
    }

    @Test
    public void exportAllExportFailureTest() throws Exception {
        CdmFieldInfo fieldInfo = populateFieldInfo();
        fieldService.persistFieldsToProject(project, fieldInfo);

        when(postStatus.getStatusCode()).thenReturn(400);
        when(postEntity.getContent()).thenReturn(new ByteArrayInputStream("bad".getBytes()));
        when(getStatus.getStatusCode()).thenReturn(200);

        try {
            service.exportAll(project);
            fail();
        } catch (MigrationException e) {
            // Should only have made one call
            verify(httpClient).execute(any());
            assertFalse("Export folder should not exist", Files.exists(project.getExportPath()));
        }
    }

    @Test
    public void exportAllDownloadFailureTest() throws Exception {
        CdmFieldInfo fieldInfo = populateFieldInfo();
        fieldService.persistFieldsToProject(project, fieldInfo);

        when(postStatus.getStatusCode()).thenReturn(200);
        when(getStatus.getStatusCode()).thenReturn(400);

        try {
            service.exportAll(project);
            fail();
        } catch (MigrationException e) {
            verify(httpClient, times(2)).execute(any());
            assertFalse("Export folder should not exist", Files.exists(project.getExportPath()));
        }
    }

    @Test
    public void exportAllSkipFieldTest() throws Exception {
        CdmFieldInfo fieldInfo = populateFieldInfo();
        fieldInfo.getFields().get(1).setSkipExport(true);
        fieldService.persistFieldsToProject(project, fieldInfo);

        when(postStatus.getStatusCode()).thenReturn(200);
        when(getStatus.getStatusCode()).thenReturn(200);

        service.exportAll(project);

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
        assertEquals(EXPORT_BODY, FileUtils.readFileToString(
                project.getExportPath().resolve("export_all.xml").toFile(), ISO_8859_1));
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
