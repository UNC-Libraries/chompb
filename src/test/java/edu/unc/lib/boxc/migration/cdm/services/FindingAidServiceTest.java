package edu.unc.lib.boxc.migration.cdm.services;

import static edu.unc.lib.boxc.migration.cdm.services.FindingAidService.CONTRI_FIELD;
import static edu.unc.lib.boxc.migration.cdm.services.FindingAidService.DESCRI_FIELD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FindingAidServiceTest {
    private static final String PROJECT_NAME = "gilmer";

    @TempDir
    public Path tmpFolder;

    private MigrationProject project;
    private CdmFieldService fieldService;
    private FindingAidService service;

    @BeforeEach
    public void setup() throws Exception {
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder, PROJECT_NAME, null, "user", CdmEnvironmentHelper.DEFAULT_ENV_ID);

        fieldService = new CdmFieldService();
        service = new FindingAidService();
        service.setCdmFieldService(fieldService);
        service.setProject(project);
    }

    @Test
    public void recordFindingAidTest() throws Exception {
        Files.copy(Paths.get("src/test/resources/findingaid_fields.csv"), project.getFieldsPath());
        service.recordFindingAid();

        assertEquals(CONTRI_FIELD, project.getProjectProperties().getHookId());
        assertEquals(DESCRI_FIELD, project.getProjectProperties().getCollectionNumber());
    }

    @Test
    public void recordFindingAidNoFieldCsvTest() throws Exception {
        try {
            service.recordFindingAid();
            fail();
        } catch (MigrationException e) {
            //Expected
        }
    }

    @Test
    public void noFindingAidToRecordTest() throws Exception {
        Files.copy(Paths.get("src/test/resources/gilmer_fields.csv"), project.getFieldsPath());
        service.recordFindingAid();

        assertNull(project.getProjectProperties().getHookId());
        assertNull(project.getProjectProperties().getCollectionNumber());
    }
}
