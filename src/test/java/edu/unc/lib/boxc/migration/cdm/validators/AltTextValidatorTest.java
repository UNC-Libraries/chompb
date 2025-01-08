package edu.unc.lib.boxc.migration.cdm.validators;

import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.AltTextService;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.test.BxcEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.SipServiceHelper;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AltTextValidatorTest {
    private static final String PROJECT_NAME = "proj";
    private static final String USERNAME = "migr_user";
    @TempDir
    public Path tmpFolder;

    private MigrationProject project;
    private AltTextValidator validator;
    private SipServiceHelper testHelper;

    @BeforeEach
    public void setup() throws Exception {
        project = MigrationProjectFactory.createCdmMigrationProject(
                tmpFolder, PROJECT_NAME, null, USERNAME,
                CdmEnvironmentHelper.DEFAULT_ENV_ID, BxcEnvironmentHelper.DEFAULT_ENV_ID);

        testHelper = new SipServiceHelper(project, tmpFolder);
        validator = new AltTextValidator();
        validator.setProject(project);
    }

    @Test
    public void noEntriesTest() throws Exception {
        Path testCsv = writeCsv(mappingBody());
        List<String> errors = validator.validateCsv(testCsv, false);
        assertHasError(errors, "Alt-text CSV contained no alt-text");
        assertNumberErrors(errors, 1);
    }

    @Test
    public void blankIdTest() throws Exception {
        Path testCsv = writeCsv(mappingBody(",alt-text"));
        List<String> errors = validator.validateCsv(testCsv, false);
        assertHasError(errors, "Invalid blank id at line 2");
        assertNumberErrors(errors, 1);
    }

    @Test
    public void blankAltTextTest() throws Exception {
        Path testCsv = writeCsv(mappingBody("25,"));
        List<String> errors = validator.validateCsv(testCsv, false);
        assertHasError(errors, "No alt-text at line 2");
        assertNumberErrors(errors, 1);
    }

    @Test
    public void tooFewColumnsTest() throws Exception {
        Path testCsv = writeCsv(mappingBody("25"));
        List<String> errors = validator.validateCsv(testCsv, false);
        assertHasError(errors, "Invalid entry at line 2, must be 2 columns but were 1");
        // Registers as having no mappings
        assertNumberErrors(errors, 2);
    }

    @Test
    public void tooManyColumnsTest() throws Exception {
        Path testCsv = writeCsv(mappingBody("25,alttext,"));
        List<String> errors = validator.validateCsv(testCsv, false);
        assertHasError(errors, "Invalid entry at line 2, must be 2 columns but were 3");
        // Registers as having no mappings
        assertNumberErrors(errors, 2);
    }

    @Test
    public void duplicateIdTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        Path testCsv = writeCsv(mappingBody("25,alt-text",
                "25,alt-text"));
        List<String> errors = validator.validateCsv(testCsv, false);
        assertHasError(errors, "Duplicate id 25 at line 3");
        assertNumberErrors(errors, 1);
    }

    @Test
    public void validMappingsTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        Path testCsv = writeCsv(mappingBody("25,alt-text",
                "26,more alt-text"));
        List<String> errors = validator.validateCsv(testCsv, false);
        assertNumberErrors(errors, 0);
    }

    @Test
    public void errorsOnMultipleLinesTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        Path testCsv = writeCsv(mappingBody(",alt-text",
                "25,alt-text,"));
        List<String> errors = validator.validateCsv(testCsv, false);
        assertHasError(errors, "Invalid blank id at line 2");
        assertHasError(errors, "Invalid entry at line 3, must be 2 columns but were 3");
        assertNumberErrors(errors, 2);
    }

    @Test
    public void ignorableErrorsWithoutForceTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        Path testCsv = writeCsv(mappingBody("25,",
                "26,alt-text"));
        List<String> errors = validator.validateCsv(testCsv, false);
        assertHasError(errors, "No alt-text at line 2");
        assertNumberErrors(errors, 1);
    }

    @Test
    public void ignorableErrorsWithForceTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        Path testCsv = writeCsv(mappingBody("25,",
                "26,alt-text"));
        List<String> errors = validator.validateCsv(testCsv, true);
        assertNumberErrors(errors, 0);
    }

    private void assertHasError(List<String> errors, String expected) {
        assertTrue(errors.contains(expected),
                "Expected error:\n" + expected + "\nBut the returned errors were:\n" + String.join("\n", errors));
    }

    private String mappingBody(String... rows) {
        return String.join(",", AltTextService.CSV_HEADERS) + "\n"
                + String.join("\n", rows);
    }

    private Path writeCsv(String mappingBody) throws IOException {
        Path csvPath = project.getAltTextCsvPath();
        FileUtils.write(csvPath.toFile(),
                mappingBody, StandardCharsets.UTF_8);
        return csvPath;
    }

    private void assertNumberErrors(List<String> errors, int expected) {
        assertEquals(expected, errors.size(),
                "Incorrect number of errors:\n" + String.join("\n", errors));
    }
}
