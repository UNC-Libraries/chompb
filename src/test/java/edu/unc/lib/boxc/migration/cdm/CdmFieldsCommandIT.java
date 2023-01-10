package edu.unc.lib.boxc.migration.cdm;

import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo.CdmFieldEntry;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.CdmFieldService;
import edu.unc.lib.boxc.migration.cdm.services.FieldAssessmentTemplateService;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.SipServiceHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author bbpennel, snluong
 */
public class CdmFieldsCommandIT extends AbstractCommandIT {

    private CdmFieldService fieldService;
    private FieldAssessmentTemplateService templateService;

    @BeforeEach
    public void setUp() throws Exception {
        fieldService = new CdmFieldService();
        templateService = new FieldAssessmentTemplateService();
        templateService.setCdmFieldService(fieldService);
    }

    @AfterEach
    public void cleanup() {
        System.setOut(originalOut);
    }

    @Test
    public void validateValidFieldsConfigTest() throws Exception {
        initProject();
        CdmFieldInfo fieldInfo = new CdmFieldInfo();
        CdmFieldEntry fieldEntry = new CdmFieldEntry();
        fieldEntry.setNickName("title");
        fieldEntry.setExportAs("titla");
        fieldEntry.setDescription("Title");
        fieldInfo.getFields().add(fieldEntry);
        fieldService.persistFieldsToProject(project, fieldInfo);

        String[] cmdArgs = new String[] {
                "-w", project.getProjectPath().toString(),
                "fields", "validate"};
        executeExpectSuccess(cmdArgs);
    }

    @Test
    public void validateInvalidFieldsConfigTest() throws Exception {
        initProject();
        CdmFieldInfo fieldInfo = new CdmFieldInfo();
        CdmFieldEntry fieldEntry = new CdmFieldEntry();
        fieldEntry.setNickName("title");
        fieldEntry.setExportAs("titla");
        fieldEntry.setDescription("Title");
        fieldInfo.getFields().add(fieldEntry);
        CdmFieldEntry fieldEntry2 = new CdmFieldEntry();
        fieldEntry2.setNickName("title2");
        fieldEntry2.setExportAs("titla");
        fieldEntry2.setDescription("Another Title");
        fieldInfo.getFields().add(fieldEntry2);
        fieldService.persistFieldsToProject(project, fieldInfo);

        String[] cmdArgs = new String[] {
                "-w", project.getProjectPath().toString(),
                "fields", "validate"};
        executeExpectFailure(cmdArgs);

        assertOutputContains("Duplicate export_as value 'titla'");
    }

    @Test
    public void generateReportTest() throws Exception {
        initProject();
        Files.copy(Paths.get("src/test/resources/gilmer_fields.csv"), project.getFieldsPath());
        templateService.generate(project);

        Path projPath = project.getProjectPath();
        Path newPath = projPath.resolve("field_assessment_my_proj.xlsx");

        assertTrue(Files.exists(newPath));
    }

    @Test
    public void generateFieldsUrlReportTest() throws Exception {
        initProjectAndHelper();
        testHelper.indexExportData("mini_gilmer");

        Path projectPath = project.getProjectPath();
        Path reportPath = projectPath.resolve("my_proj_field_urls.csv");

        String[] cmdArgs = new String[] {
                "-w", projectPath.toString(),
                "fields", "generate_field_url_report"};
        executeExpectSuccess(cmdArgs);

        assertTrue(Files.exists(reportPath));
    }
}
