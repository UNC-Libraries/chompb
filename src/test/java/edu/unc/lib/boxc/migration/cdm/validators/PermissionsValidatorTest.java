package edu.unc.lib.boxc.migration.cdm.validators;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.PermissionsInfo;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.services.PermissionsService;
import edu.unc.lib.boxc.migration.cdm.test.BxcEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
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

public class PermissionsValidatorTest {
    private static final String PROJECT_NAME = "proj";
    private static final String USERNAME = "migr_user";
    @TempDir
    public Path tmpFolder;

    private MigrationProject project;
    private PermissionsValidator validator;
    private PermissionsService permissionsService;

    @BeforeEach
    public void setup() throws Exception {
        project = MigrationProjectFactory.createCdmMigrationProject(
                tmpFolder, PROJECT_NAME, null, USERNAME,
                CdmEnvironmentHelper.DEFAULT_ENV_ID, BxcEnvironmentHelper.DEFAULT_ENV_ID);

        validator = new PermissionsValidator();
        validator.setProject(project);
        permissionsService = new PermissionsService();
        permissionsService.setProject(project);
    }

    @Test
    public void validMappingsTest() throws Exception {
        writeCsv(mappingBody("25,work,none,none"));
        List<String> errors = validator.validateMappings();
        assertNumberErrors(errors, 0);
    }

    @Test
    public void noMappingFileTest() throws Exception {
        Assertions.assertThrows(MigrationException.class, () -> {
            validator.validateMappings();
        });
    }

    @Test
    public void noEntriesTest() throws Exception {
        writeCsv(mappingBody());
        List<String> errors = validator.validateMappings();
        assertHasError(errors, "Permission mappings file contained no mappings");
        assertNumberErrors(errors, 1);
    }

    @Test
    public void blankIdTest() throws Exception {
        writeCsv(mappingBody(",,none,none"));
        List<String> errors = validator.validateMappings();
        assertHasError(errors, "Invalid blank id at line 2");
        assertNumberErrors(errors, 1);
    }

    @Test
    public void blankEveryoneTest() throws Exception {
        writeCsv(mappingBody("25,work,,none"));
        List<String> errors = validator.validateMappings();
        assertHasError(errors, "No 'everyone' permission mapped at line 2");
        assertNumberErrors(errors, 1);
    }

    @Test
    public void invalidEveryoneTest() throws Exception {
        writeCsv(mappingBody("default,,okaynope,none"));
        List<String> errors = validator.validateMappings();
        assertHasError(errors, "Invalid 'everyone' permission at line 2, okaynope is not a valid patron permission");
        assertNumberErrors(errors, 1);
    }

    @Test
    public void blankAuthenticatedTest() throws Exception {
        writeCsv(mappingBody("default,,none,"));
        List<String> errors = validator.validateMappings();
        assertHasError(errors, "No 'authenticated' permission mapped at line 2");
        assertNumberErrors(errors, 1);
    }

    @Test
    public void invalidAuthenticatedTest() throws Exception {
        writeCsv(mappingBody("26,work,none,okaynope"));
        List<String> errors = validator.validateMappings();
        assertHasError(errors, "Invalid 'authenticated' permission at line 2, " +
                "okaynope is not a valid patron permission");
        assertNumberErrors(errors, 1);
    }

    @Test
    public void multipleDefaultsTest() throws Exception {
        writeCsv(mappingBody("default,,none,none",
                "default,,canViewOriginals,canViewOriginals"));
        List<String> errors = validator.validateMappings();
        assertHasErrorMatching(errors, "Can only map default permissions once.*at line 3");
        assertNumberErrors(errors, 1);
    }

    @Test
    public void tooFewColumnsTest() throws Exception {
        writeCsv(mappingBody("default,none,"));
        List<String> errors = validator.validateMappings();
        assertHasError(errors, "Invalid entry at line 2, must be 4 columns but were 3");
        assertNumberErrors(errors, 1);
    }

    @Test
    public void tooManyColumnsTest() throws Exception {
        writeCsv(mappingBody("25,work,none,none,none"));
        List<String> errors = validator.validateMappings();
        assertHasError(errors, "Invalid entry at line 2, must be 4 columns but were 5");
        assertNumberErrors(errors, 1);
    }

    @Test
    public void errorsOnSameLineTest() throws Exception {
        writeCsv(mappingBody("26,,okaynope,"));
        List<String> errors = validator.validateMappings();
        assertHasError(errors, "Invalid 'everyone' permission at line 2, " +
                "okaynope is not a valid patron permission");
        assertHasErrorMatching(errors, "No 'authenticated' permission mapped at line 2");
        assertNumberErrors(errors, 2);
    }

    private void assertHasError(List<String> errors, String expected) {
        assertTrue(errors.contains(expected),
                "Expected error:\n" + expected + "\nBut the returned errors were:\n" +
                        String.join("\n", errors));
    }

    private void assertHasErrorMatching(List<String> errors, String expectedP) {
        assertTrue(errors.stream().anyMatch(e -> e.matches(expectedP)),
                "Expected error:\n" + expectedP + "\nBut the returned errors were:\n" +
                        String.join("\n", errors));
    }

    private String mappingBody(String... rows) {
        return String.join(",", PermissionsInfo.CSV_HEADERS) + "\n"
                + String.join("\n", rows);
    }

    private void writeCsv(String mappingBody) throws IOException {
        FileUtils.write(project.getPermissionsPath().toFile(),
                mappingBody, StandardCharsets.UTF_8);
    }

    private void assertNumberErrors(List<String> errors, int expected) {
        assertEquals(expected, errors.size(),
                "Incorrect number of errors:\n" + String.join("\n", errors));
    }
}
