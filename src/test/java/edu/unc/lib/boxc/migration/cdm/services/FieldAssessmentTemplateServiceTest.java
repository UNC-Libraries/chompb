package edu.unc.lib.boxc.migration.cdm.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.ss.usermodel.Sheet;

import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;

/**
 * @author krwong
 */
public class FieldAssessmentTemplateServiceTest {
    private static final String PROJECT_NAME = "gilmer";
    @TempDir
    public Path tmpFolder;

    private MigrationProject project;
    private CdmFieldService fieldService;
    private FieldAssessmentTemplateService service;

    @BeforeEach
    public void setup() throws Exception {
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder.getRoot(), PROJECT_NAME, null, "user", CdmEnvironmentHelper.DEFAULT_ENV_ID);
        fieldService = new CdmFieldService();

        service = new FieldAssessmentTemplateService();
        service.setCdmFieldService(fieldService);
    }

    @Test
    public void allExpectedCellsPopulatedTest() throws Exception {
        populateFieldInfo();
        service.generate(project);

        Path projPath = project.getProjectPath();
        InputStream inputStream = Files.newInputStream(projPath.resolve("field_assessment_gilmer.xlsx"));
        XSSFWorkbook workbook = new XSSFWorkbook(inputStream);
        Sheet sheet = workbook.getSheetAt(0);

        assertEquals(60, sheet.getLastRowNum());
        assertEquals(16, sheet.getRow(0).getPhysicalNumberOfCells());
        assertEquals(12, sheet.getRow(1).getPhysicalNumberOfCells());
        assertEquals(12, sheet.getRow(60).getPhysicalNumberOfCells());
        assertNull(sheet.getRow(1).getCell(2));
        assertNull(sheet.getRow(1).getCell(8));
        assertNull(sheet.getRow(1).getCell(9));
        assertNull(sheet.getRow(1).getCell(10));
        assertNotEquals("Search by Decade", workbook.getSheetAt(0).getRow(7).getCell(0));
        assertNotEquals("search", workbook.getSheetAt(0).getRow(7).getCell(1));
    }

    @Test
    public void regenerateTemplateTest() throws Exception {
        populateFieldInfo();
        service.generate(project);
        service.generate(project);

        Path projPath = project.getProjectPath();
        Path newPath = projPath.resolve("field_assessment_gilmer.xlsx");

        assertTrue(Files.exists(newPath));
    }

    @Test
    public void invalidFieldsTest() throws Exception {
        try {
            service.generate(project);
            fail();
        } catch (InvalidProjectStateException e) {
            assertTrue(e.getMessage().contains("CDM fields file is missing"));
        }
    }

    private void populateFieldInfo() throws Exception {
        Files.copy(Paths.get("src/test/resources/gilmer_fields.csv"), project.getFieldsPath());
    }
}
