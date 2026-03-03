package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo.CdmFieldEntry;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.CdmExportOptions;
import edu.unc.lib.boxc.migration.cdm.services.export.ExportStateService;
import edu.unc.lib.boxc.migration.cdm.test.BxcEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static edu.unc.lib.boxc.migration.cdm.test.EadToCdmHelper.getJsonContent;
import static edu.unc.lib.boxc.migration.cdm.test.IndexServiceHelper.mappingBody;
import static edu.unc.lib.boxc.migration.cdm.test.IndexServiceHelper.writeCsv;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

/**
 * @author bbpennel
 */
public class CdmExportServiceTest {
    private static final String PROJECT_NAME = "proj";
    private AutoCloseable closeable;
    @TempDir
    public Path tmpFolder;

    private MigrationProject project;
    private CdmFieldService fieldService;
    private CdmExportService service;
    private ChompbConfigService.ChompbConfig chompbConfig;
    private ExportStateService exportStateService;
    @Mock
    private CdmFileRetrievalService cdmFileRetrievalService;
    private String descAllResourcePath = "/descriptions/gilmer/index/description/desc.all";
    private String descAllPath = "src/test/resources" + descAllResourcePath;
    @Mock
    private CloseableHttpClient httpClient;

    @BeforeEach
    public void setup() throws Exception {
        closeable = openMocks(this);
        project = MigrationProjectFactory.createCdmMigrationProject(
                tmpFolder, PROJECT_NAME, null, "user",
                CdmEnvironmentHelper.DEFAULT_ENV_ID, BxcEnvironmentHelper.DEFAULT_ENV_ID);
        fieldService = new CdmFieldService();
        exportStateService = new ExportStateService();
        exportStateService.setProject(project);
        exportStateService.transitionToStarting();
        service = new CdmExportService();
        service.setProject(project);
        service.setCdmFieldService(fieldService);
        service.setExportStateService(exportStateService);
        service.setFileRetrievalService(cdmFileRetrievalService);
        var chompbConfig = new ChompbConfigService.ChompbConfig();
        chompbConfig.setCdmEnvironments(CdmEnvironmentHelper.getTestMapping());
        chompbConfig.setBxcEnvironments(BxcEnvironmentHelper.getTestMapping());
        service.setChompbConfig(chompbConfig);

        // Trigger population of desc.all file
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                FileUtils.copyFile(new File(descAllPath),
                        project.getExportPath().resolve(CdmFileRetrievalService.DESC_ALL_FILENAME).toFile());
                return null;
            }
        }).when(cdmFileRetrievalService).downloadDescAllFile();
    }

    @AfterEach
    void closeService() throws Exception {
        closeable.close();
    }

    private CdmExportOptions makeExportOptions() {
        CdmExportOptions options = new CdmExportOptions();
        options.setCdmUsername("user");
        return options;
    }

    @Test
    public void exportAllValidProjectTest() throws Exception {
        CdmFieldInfo fieldInfo = populateFieldInfo();
        fieldService.persistFieldsToProject(project, fieldInfo);

        service.exportAll(makeExportOptions());

        assertTrue(Files.exists(project.getExportPath()), "Export folder not created");
        assertExportedFileContentsMatch(descAllResourcePath);
    }

    @Test
    public void exportAllExportFailureTest() throws Exception {
        doThrow(new MigrationException("Failed to download")).when(cdmFileRetrievalService).downloadDescAllFile();

        CdmFieldInfo fieldInfo = populateFieldInfo();
        fieldService.persistFieldsToProject(project, fieldInfo);

        try {
            service.exportAll(makeExportOptions());
            fail();
        } catch (MigrationException e) {
            // no files should be present
            assertFalse(Files.exists(CdmFileRetrievalService.getDescAllPath(project)));
        }
    }

    @Test
    public void exportFromEadToCdmBodyTest() throws Exception {
        CdmFieldInfo fieldInfo = populateFieldInfo();
        fieldService.persistFieldsToProject(project, fieldInfo);
        var options = makeExportOptions();
        options.setEadToCdm(true);
        options.setEadId("00001");
        writeCsv(project, mappingBody("00001,," + project.getProjectPath() + "/02096-z_0001_0001.tif,"));
        var builder = mock(HttpClientBuilder.class);
        StringEntity stringEntity = new StringEntity(getJsonContent(), ContentType.APPLICATION_JSON);

        try (MockedStatic<HttpClientBuilder> mockedStatic = mockStatic(HttpClientBuilder.class) ) {
            mockedStatic.when(HttpClientBuilder::create).thenReturn(builder);
            when(builder.disableRedirectHandling()).thenReturn(builder);
            when(builder.setDefaultCredentialsProvider(any())).thenReturn(builder);
            when(builder.build()).thenReturn(httpClient);
            var resp = mock(CloseableHttpResponse.class);
            when(httpClient.execute(any())).thenReturn(resp);
            when(resp.getEntity()).thenReturn(stringEntity);
            service.exportAll(options);

            var postArgument = getHttpPostArgument();
            var jsonString = IOUtils.toString(postArgument.getEntity().getContent(), StandardCharsets.UTF_8);
            assertEquals( "{\"ead_id\":\"00001\",\"files\":\"02096-z_0001_0001.tif\"}", jsonString);
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

    private void assertExportedFileContentsMatch(String expectContentPath) throws Exception {
        assertEquals(IOUtils.toString(getClass().getResourceAsStream(expectContentPath), StandardCharsets.UTF_8),
                FileUtils.readFileToString(CdmFileRetrievalService.getDescAllPath(project).toFile(), StandardCharsets.UTF_8));
    }

    private HttpPost getHttpPostArgument() throws Exception {
        ArgumentCaptor<HttpPost> httpPostCaptor = ArgumentCaptor.forClass(HttpPost.class);
        verify(httpClient).execute(httpPostCaptor.capture());
        return httpPostCaptor.getValue();
    }
}
