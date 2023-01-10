package edu.unc.lib.boxc.migration.cdm.validators;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.DestinationsInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.DestinationsService;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;

/**
 * @author bbpennel
 */
public class DestinationsValidatorTest {
    private static final String PROJECT_NAME = "proj";
    private static final String USERNAME = "migr_user";
    @TempDir
    public Path tmpFolder;

    private MigrationProject project;
    private DestinationsValidator validator;
    private DestinationsService destService;

    @BeforeEach
    public void setup() throws Exception {
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder, PROJECT_NAME, null, USERNAME, CdmEnvironmentHelper.DEFAULT_ENV_ID);

        validator = new DestinationsValidator();
        validator.setProject(project);
        destService = new DestinationsService();
        destService.setProject(project);
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
        assertHasError(errors, "Destination mappings file contained no mappings");
        assertNumberErrors(errors, 1);
    }

    @Test
    public void blankIdTest() throws Exception {
        writeCsv(mappingBody(",3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,"));
        List<String> errors = validator.validateMappings(false);
        assertHasError(errors, "Invalid blank id at line 2");
        assertNumberErrors(errors, 1);
    }

    @Test
    public void blankDestinationTest() throws Exception {
        writeCsv(mappingBody("55,,"));
        List<String> errors = validator.validateMappings(false);
        assertHasError(errors, "No destination mapped at line 2");
        assertNumberErrors(errors, 1);
    }

    @Test
    public void invalidDestinationTest() throws Exception {
        writeCsv(mappingBody("55,okaynope,"));
        List<String> errors = validator.validateMappings(false);
        assertHasError(errors, "Invalid destination at line 2, okaynope is not a valid UUID");
        assertNumberErrors(errors, 1);
    }

    @Test
    public void newCollAssignedToMultipleDestsTest() throws Exception {
        writeCsv(mappingBody("55,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,0012345",
                             "67,126def1e-852b-4ca5-9453-26e61db33598,0012345"));
        List<String> errors = validator.validateMappings(false);
        assertHasErrorMatching(errors, "New collection ID 0012345 cannot be associated with multiple dest.*");
        assertNumberErrors(errors, 1);
    }

    @Test
    public void multiDefaultsTest() throws Exception {
        writeCsv(mappingBody("default,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,",
                             "default,126def1e-852b-4ca5-9453-26e61db33598,"));
        List<String> errors = validator.validateMappings(false);
        assertHasErrorMatching(errors, "Can only map default destination once.*at line 3");
        assertNumberErrors(errors, 1);
    }

    @Test
    public void tooFewColumnsTest() throws Exception {
        writeCsv(mappingBody("55,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e",
                             "66,126def1e-852b-4ca5-9453-26e61db33598,"));
        List<String> errors = validator.validateMappings(false);
        assertHasError(errors, "Invalid entry at line 2, must be 3 columns but were 2");
        assertNumberErrors(errors, 1);
    }

    @Test
    public void tooManyColumnsTest() throws Exception {
        writeCsv(mappingBody("55,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,",
                             "66,126def1e-852b-4ca5-9453-26e61db33598,01234,56789"));
        List<String> errors = validator.validateMappings(false);
        assertHasError(errors, "Invalid entry at line 3, must be 3 columns but were 4");
        assertNumberErrors(errors, 1);
    }

    @Test
    public void assignSameAsDefaultTest() throws Exception {
        writeCsv(mappingBody("default,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,",
                             "55,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,"));
        List<String> errors = validator.validateMappings(false);
        assertHasErrorMatching(errors, "Destination at line 3 is already mapped as default");
        assertNumberErrors(errors, 1);
    }

    @Test
    public void assignDefaultToAlreadyAssignedTest() throws Exception {
        writeCsv(mappingBody("55,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,",
                             "default,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,"));
        List<String> errors = validator.validateMappings(false);
        assertHasErrorMatching(errors, "Default destination .* already mapped.*");
        assertNumberErrors(errors, 1);
    }

    @Test
    public void objectIdMultipleDestsTest() throws Exception {
        writeCsv(mappingBody("55,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,",
                             "55,126def1e-852b-4ca5-9453-26e61db33598,"));
        List<String> errors = validator.validateMappings(false);
        assertHasError(errors, "Object ID assigned to multiple destinations, see line 3");
        assertNumberErrors(errors, 1);
    }

    @Test
    public void sameDestWithAndWithoutCollTest() throws Exception {
        writeCsv(mappingBody("50,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,",
                             "55,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,34521",
                             "58,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,"));
        List<String> errors = validator.validateMappings(false);
        assertHasErrorMatching(errors, "Destination at line 3 .* mapped without a new collection");
        assertNumberErrors(errors, 1);
    }

    @Test
    public void sameDestWithoutAndWithCollTest() throws Exception {
        writeCsv(mappingBody("50,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,12356",
                             "55,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,",
                             "58,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,12356"));
        List<String> errors = validator.validateMappings(false);
        assertHasErrorMatching(errors, "Destination at line 3 .* mapped with a new collection");
        assertNumberErrors(errors, 1);
    }

    @Test
    public void sameDestMultipleNewCollectionsValidTest() throws Exception {
        writeCsv(mappingBody("55,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,04567",
                             "57,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,01234"));
        List<String> errors = validator.validateMappings(false);
        assertNumberErrors(errors, 0);
    }

    @Test
    public void validMappingsTest() throws Exception {
        writeCsv(mappingBody("default,126def1e-852b-4ca5-9453-26e61db33598,56789",
                             "55,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,",
                             "57,126def1e-852b-4ca5-9453-26e61db33598,01234",
                             "59,9ee8de0d-59ae-4c67-9686-78a79ebc93b1,"));
        List<String> errors = validator.validateMappings(false);
        assertNumberErrors(errors, 0);
    }

    @Test
    public void errorsOnMultipleLinesTest() throws Exception {
        writeCsv(mappingBody("default,9ee8de0d-59ae-4c67-9686-78a79ebc93b1,",
                             "55,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,",
                             "57,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,01234",
                             "59,9ee8de0d-59ae-4c67-9686-78a79ebc93b1,"));
        List<String> errors = validator.validateMappings(false);
        assertHasErrorMatching(errors, "Destination at line 4 .* previously mapped without a new collection");
        assertHasErrorMatching(errors, "Destination at line 5 is already mapped as default");
        assertNumberErrors(errors, 2);
    }

    @Test
    public void errorsOnSameLineTest() throws Exception {
        writeCsv(mappingBody("default,9ee8de0d-59ae-4c67-9686-78a79ebc93b1,",
                             "55,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,",
                             "55,9ee8de0d-59ae-4c67-9686-78a79ebc93b1,"));
        List<String> errors = validator.validateMappings(false);
        assertHasError(errors, "Object ID assigned to multiple destinations, see line 4");
        assertHasErrorMatching(errors, "Destination at line 4 is already mapped as default");
        assertNumberErrors(errors, 2);
    }

    @Test
    public void ignorableErrorsWithoutForceTest() throws Exception {
        writeCsv(mappingBody("55,9ee8de0d-59ae-4c67-9686-78a79ebc93b1,",
                             "default,9ee8de0d-59ae-4c67-9686-78a79ebc93b1,",
                             ",3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,",
                             "57,,",
                             "59,9ee8de0d-59ae-4c67-9686-78a79ebc93b1,"));
        List<String> errors = validator.validateMappings(false);
        assertHasErrorMatching(errors, "Default destination .* is already mapped directly to objects");
        assertHasError(errors, "Invalid blank id at line 4");
        assertHasError(errors, "No destination mapped at line 5");
        assertHasErrorMatching(errors, "Destination at line 6 is already mapped as default");
        assertNumberErrors(errors, 4);
    }

    @Test
    public void ignorableErrorsWithForceTest() throws Exception {
        writeCsv(mappingBody("55,9ee8de0d-59ae-4c67-9686-78a79ebc93b1,",
                             "default,9ee8de0d-59ae-4c67-9686-78a79ebc93b1,",
                             ",3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,",
                             "57,,",
                             "59,9ee8de0d-59ae-4c67-9686-78a79ebc93b1,"));
        List<String> errors = validator.validateMappings(true);
        assertNumberErrors(errors, 0);
    }

    @Test
    public void mixedErrorsWithForceTest() throws Exception {
        writeCsv(mappingBody("55,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,",
                             "57,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,12345",
                             "59,,"));
        List<String> errors = validator.validateMappings(true);
        assertHasErrorMatching(errors, "Destination at line 3 .* mapped without a new collection");
        assertNumberErrors(errors, 1);
    }

    private void assertHasError(List<String> errors, String expected) {
        assertTrue(errors.contains(expected),
                "Expected error:\n" + expected + "\nBut the returned errors were:\n" + String.join("\n", errors));
    }

    private void assertHasErrorMatching(List<String> errors, String expectedP) {
        assertTrue(errors.stream().anyMatch(e -> e.matches(expectedP)),
                "Expected error:\n" + expectedP + "\nBut the returned errors were:\n" + String.join("\n", errors));
    }

    private String mappingBody(String... rows) {
        return String.join(",", DestinationsInfo.CSV_HEADERS) + "\n"
                + String.join("\n", rows);
    }

    private void writeCsv(String mappingBody) throws IOException {
        FileUtils.write(project.getDestinationMappingsPath().toFile(),
                mappingBody, StandardCharsets.UTF_8);
    }

    private void assertNumberErrors(List<String> errors, int expected) {
        assertEquals(expected, errors.size(),
                "Incorrect number of errors:\n" + String.join("\n", errors));
    }
}
