package edu.unc.lib.boxc.migration.cdm.validators;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.AspaceRefIdInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.test.BxcEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.SipServiceHelper;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AspaceRefIdValidatorTest {
    private static final String PROJECT_NAME = "proj";
    private static final String USERNAME = "migr_user";
    @TempDir
    public Path tmpFolder;

    private MigrationProject project;
    private AspaceRefIdValidator validator;
    private SipServiceHelper testHelper;

    @BeforeEach
    public void setup() throws Exception {
        project = MigrationProjectFactory.createCdmMigrationProject(
                tmpFolder, PROJECT_NAME, null, USERNAME,
                CdmEnvironmentHelper.DEFAULT_ENV_ID, BxcEnvironmentHelper.DEFAULT_ENV_ID);

        testHelper = new SipServiceHelper(project, tmpFolder);

        validator = new AspaceRefIdValidator();
        validator.setProject(project);
    }

    @Test
    public void validMappingTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        writeCsv(mappingBody("25,fcee5fc2bb61effc8836498a8117b05d"));
        List<String> errors = validator.validateMappings(false);
        assertNumberErrors(errors, 0);
    }

    @Test
    public void noMappingFileTest() throws Exception {
        Assertions.assertThrows(MigrationException.class, () -> {
            validator.validateMappings(false);
        });
    }

    @Test
    public void noEntriesTest() throws Exception {
        writeCsv(mappingBody());
        List<String> errors = validator.validateMappings(false);
        assertHasError(errors, "Mappings file contained no mappings");
        assertNumberErrors(errors, 1);
    }

    @Test
    public void blankIdTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        writeCsv(mappingBody(",fcee5fc2bb61effc8836498a8117b05d"));
        List<String> errors = validator.validateMappings(false);
        assertHasError(errors, "Invalid blank id at line 2");
        assertNumberErrors(errors, 1);
    }

    @Test
    public void blankIdForceTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        writeCsv(mappingBody(",fcee5fc2bb61effc8836498a8117b05d"));
        List<String> errors = validator.validateMappings(true);
        assertNumberErrors(errors, 0);
    }

    @Test
    public void blankRefIdTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        writeCsv(mappingBody("25,"));
        List<String> errors = validator.validateMappings(false);
        assertNumberErrors(errors, 0);
    }

    @Test
    public void tooFewColumnsTest() throws Exception {
        writeCsv(mappingBody("25"));
        List<String> errors = validator.validateMappings(false);
        assertHasError(errors, "Invalid entry at line 2, must be 2 or 3 columns but were 1");
        // Registers as having no mappings
        assertNumberErrors(errors, 2);
    }

    @Test
    public void tooManyColumnsTest() throws Exception {
        writeCsv(mappingBody("25,,,"));
        List<String> errors = validator.validateMappings(false);
        assertHasError(errors, "Invalid entry at line 2, must be 2 or 3 columns but were 4");
        // Registers as having no mappings
        assertNumberErrors(errors, 2);
    }

    @Test
    public void duplicateIdTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        writeCsv(mappingBody("25,fcee5fc2bb61effc8836498a8117b05d",
                "25,4817ec3c77e5ea9846d5c070d58d402b"));
        List<String> errors = validator.validateMappings(false);
        assertHasError(errors, "Duplicate mapping for id 25 at line 3");
        assertNumberErrors(errors, 1);
    }

    @Test
    public void duplicateRefIdTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        writeCsv(mappingBody("25,fcee5fc2bb61effc8836498a8117b05d",
                "26,fcee5fc2bb61effc8836498a8117b05d"));
        List<String> errors = validator.validateMappings(false);
        assertNumberErrors(errors, 0);
    }

    private void assertHasError(List<String> errors, String expected) {
        assertTrue(errors.contains(expected),
                "Expected error:\n" + expected + "\nBut the returned errors were:\n" + String.join("\n", errors));
    }

    private String mappingBody(String... rows) {
        return String.join(",", AspaceRefIdInfo.BLANK_CSV_HEADERS) + "\n"
                + String.join("\n", rows);
    }

    private void writeCsv(String mappingBody) throws IOException {
        FileUtils.write(project.getAspaceRefIdMappingPath().toFile(),
                mappingBody, StandardCharsets.UTF_8);
    }

    private void assertNumberErrors(List<String> errors, int expected) {
        assertEquals(expected, errors.size(),
                "Incorrect number of errors:\n" + String.join("\n", errors));
    }
}
