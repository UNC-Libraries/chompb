package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo.CdmFieldEntry;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.CdmExportOptions;
import edu.unc.lib.boxc.migration.cdm.services.export.ExportStateService;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
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

    @BeforeEach
    public void setup() throws Exception {
        closeable = openMocks(this);
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder, PROJECT_NAME, null, "user", CdmEnvironmentHelper.DEFAULT_ENV_ID);
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
}
