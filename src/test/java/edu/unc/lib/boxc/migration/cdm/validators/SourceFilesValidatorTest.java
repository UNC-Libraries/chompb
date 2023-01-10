package edu.unc.lib.boxc.migration.cdm.validators;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.test.SipServiceHelper;
import org.junit.jupiter.api.io.TempDir;

/**
 * @author bbpennel
 */
public class SourceFilesValidatorTest {
    private static final String PROJECT_NAME = "proj";
    private static final String USERNAME = "migr_user";
    @TempDir
    public Path tmpFolder;

    private MigrationProject project;
    private SourceFilesValidator validator;
    private SipServiceHelper testHelper;

    @BeforeEach
    public void setup() throws Exception {
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder, PROJECT_NAME, null, USERNAME, CdmEnvironmentHelper.DEFAULT_ENV_ID);

        validator = new SourceFilesValidator();
        validator.setProject(project);
        testHelper = new SipServiceHelper(project, tmpFolder.getRoot());
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
        Path path = testHelper.addSourceFile("25.txt");
        writeCsv(mappingBody(",," + path + ","));
        List<String> errors = validator.validateMappings(false);
        assertHasError(errors, "Invalid blank id at line 2");
        assertNumberErrors(errors, 1);
    }

    @Test
    public void blankPathTest() throws Exception {
        writeCsv(mappingBody("25,,,"));
        List<String> errors = validator.validateMappings(false);
        assertHasError(errors, "No path mapped at line 2");
        assertNumberErrors(errors, 1);
    }

    @Test
    public void invalidRelativePathTest() throws Exception {
        writeCsv(mappingBody("25,,path/is/relative.txt,"));
        List<String> errors = validator.validateMappings(false);
        assertHasError(errors, "Invalid path at line 2, path is not absolute");
        assertNumberErrors(errors, 1);
    }

    @Test
    public void pathDoesNotExistTest() throws Exception {
        Path path = testHelper.addSourceFile("25.txt");
        Files.delete(path);
        writeCsv(mappingBody("25,," + path + ","));
        List<String> errors = validator.validateMappings(false);
        assertHasError(errors, "Invalid path at line 2, file does not exist");
        assertNumberErrors(errors, 1);
    }

    @Test
    public void pathIsDirectoryTest() throws Exception {
        Path path = testHelper.addSourceFile("25.txt");
        Files.delete(path);
        Files.createDirectory(path);
        writeCsv(mappingBody("25,," + path + ","));
        List<String> errors = validator.validateMappings(false);
        assertHasError(errors, "Invalid path at line 2, path is a directory");
        assertNumberErrors(errors, 1);
    }

    @Test
    public void tooFewColumnsTest() throws Exception {
        Path path = testHelper.addSourceFile("25.txt");
        writeCsv(mappingBody("25,," + path));
        List<String> errors = validator.validateMappings(false);
        assertHasError(errors, "Invalid entry at line 2, must be 4 columns but were 3");
        // Registers as having no mappings
        assertNumberErrors(errors, 2);
    }

    @Test
    public void tooManyColumnsTest() throws Exception {
        Path path = testHelper.addSourceFile("25.txt");
        writeCsv(mappingBody("25,," + path + ",,"));
        List<String> errors = validator.validateMappings(false);
        assertHasError(errors, "Invalid entry at line 2, must be 4 columns but were 5");
        // Registers as having no mappings
        assertNumberErrors(errors, 2);
    }

    @Test
    public void duplicateIdTest() throws Exception {
        Path path1 = testHelper.addSourceFile("25.txt");
        Path path2 = testHelper.addSourceFile("26.txt");
        writeCsv(mappingBody("25,," + path1 + ",",
                "25,," + path2 + ","));
        List<String> errors = validator.validateMappings(false);
        assertHasError(errors, "Duplicate mapping for id 25 at line 3");
        assertNumberErrors(errors, 1);
    }

    @Test
    public void duplicatePathTest() throws Exception {
        Path path1 = testHelper.addSourceFile("25.txt");
        writeCsv(mappingBody("25,," + path1 + ",",
                "26,," + path1 + ","));
        List<String> errors = validator.validateMappings(false);
        assertHasError(errors, "Duplicate mapping for path " + path1 + " at line 3");
        assertNumberErrors(errors, 1);
    }

    @Test
    public void validMappingsTest() throws Exception {
        Path path1 = testHelper.addSourceFile("25.txt");
        Path path2 = testHelper.addSourceFile("26.txt");
        writeCsv(mappingBody("25,," + path1 + ",",
                "26,," + path2 + ","));
        List<String> errors = validator.validateMappings(false);
        assertNumberErrors(errors, 0);
    }

    @Test
    public void errorsOnMultipleLinesTest() throws Exception {
        Path path1 = testHelper.addSourceFile("25.txt");
        Path path2 = testHelper.addSourceFile("26.txt");
        writeCsv(mappingBody(",," + path1 + ",",
                             "25,," + path2 + ",,"));
        List<String> errors = validator.validateMappings(false);
        assertHasError(errors, "Invalid blank id at line 2");
        assertHasError(errors, "Invalid entry at line 3, must be 4 columns but were 5");
        assertNumberErrors(errors, 2);
    }

    @Test
    public void errorsOnSameLineTest() throws Exception {
        Path path1 = testHelper.addSourceFile("25.txt");
        writeCsv(mappingBody("25,," + path1 + ",",
                             "25,," + path1 + ","));
        List<String> errors = validator.validateMappings(false);
        assertHasError(errors, "Duplicate mapping for id 25 at line 3");
        assertHasError(errors, "Duplicate mapping for path " + path1 + " at line 3");
        assertNumberErrors(errors, 2);
    }

    @Test
    public void ignorableErrorsWithoutForceTest() throws Exception {
        Path path2 = testHelper.addSourceFile("26.txt");
        writeCsv(mappingBody("25,,,",
                             "26,," + path2 + ","));
        List<String> errors = validator.validateMappings(false);
        assertHasError(errors, "No path mapped at line 2");
        assertNumberErrors(errors, 1);
    }

    @Test
    public void ignorableErrorsWithForceTest() throws Exception {
        Path path2 = testHelper.addSourceFile("26.txt");
        writeCsv(mappingBody("25,,,",
                             "26,," + path2 + ","));
        List<String> errors = validator.validateMappings(true);
        assertNumberErrors(errors, 0);
    }

    private void assertHasError(List<String> errors, String expected) {
        assertTrue(errors.contains(expected),
                "Expected error:\n" + expected + "\nBut the returned errors were:\n" + String.join("\n", errors));
    }

    private String mappingBody(String... rows) {
        return String.join(",", SourceFilesInfo.CSV_HEADERS) + "\n"
                + String.join("\n", rows);
    }

    private void writeCsv(String mappingBody) throws IOException {
        FileUtils.write(project.getSourceFilesMappingPath().toFile(),
                mappingBody, StandardCharsets.UTF_8);
    }

    private void assertNumberErrors(List<String> errors, int expected) {
        assertEquals(expected, errors.size(),
                "Incorrect number of errors:\n" + String.join("\n", errors));
    }
}
