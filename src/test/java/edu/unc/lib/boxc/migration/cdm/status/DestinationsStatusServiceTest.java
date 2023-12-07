package edu.unc.lib.boxc.migration.cdm.status;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;

import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import edu.unc.lib.boxc.migration.cdm.AbstractOutputTest;
import edu.unc.lib.boxc.migration.cdm.model.DestinationsInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.Verbosity;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.test.SipServiceHelper;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import org.junit.jupiter.api.io.TempDir;

/**
 * @author bbpennel
 */
public class DestinationsStatusServiceTest extends AbstractOutputTest {
    private static final String PROJECT_NAME = "proj";
    private static final String USERNAME = "migr_user";

    @TempDir
    public Path tmpFolder;

    private MigrationProject project;
    private SipServiceHelper testHelper;
    private DestinationsStatusService statusService;

    @BeforeEach
    public void setup() throws Exception {
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder, PROJECT_NAME, null, USERNAME, CdmEnvironmentHelper.DEFAULT_ENV_ID);

        testHelper = new SipServiceHelper(project, tmpFolder);
        statusService = new DestinationsStatusService();
        statusService.setProject(project);
    }

    @Test
    public void destinationsNotGeneratedTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        statusService.report(Verbosity.NORMAL);

        assertOutputMatches(".*Last Generated: +Not completed.*");
    }

    @Test
    public void destinationsNotValidTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        writeCsv(mappingBody("default,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,",
                             "27,3f3c5bcf-d5d6-46ad-,"));

        statusService.report(Verbosity.NORMAL);

        assertOutputMatches(".*Last Generated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Objects Mapped: +3 \\(100.0%\\).*");
        assertOutputMatches(".*Unmapped Objects: +0 \\(0.0%\\).*");
        assertOutputMatches(".*Unknown Objects: +0.*");
        assertOutputMatches(".*Destinations Valid: +No.*");
        assertOutputMatches(".*To Default: +2 \\(66.7%\\).*");
        assertOutputMatches(".*Destinations: +2\n.*");
        assertOutputMatches(".*Destinations:.*\n +\\* 3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e.*");
        assertOutputMatches(".*New Collections: +0\n.*");
    }

    @Test
    public void destinationsNotValidVerboseTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        writeCsv(mappingBody("default,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,",
                             "27,3f3c5bcf-d5d6-46ad-,"));

        statusService.report(Verbosity.VERBOSE);

        assertOutputMatches(".*Last Generated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Objects Mapped: +3 \\(100.0%\\).*");
        assertOutputMatches(".*Unmapped Objects: +0 \\(0.0%\\).*");
        assertOutputMatches(".*Unknown Objects: +0.*");
        assertOutputMatches(".*Destinations Valid: +No.*");
        assertOutputMatches(".*To Default: +2 \\(66.7%\\).*");
        assertOutputMatches(".*Destinations: +2\n.*");
        assertOutputMatches(".*Destinations:.*\n +\\* 3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e.*");
        assertOutputMatches(".*New Collections: +0\n.*");
        assertOutputMatches(".*Invalid destination at line 3, .* is not a valid UUID.*");
    }

    @Test
    public void destinationsNotValidQuietTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        writeCsv(mappingBody("default,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,",
                             "27,3f3c5bcf-d5d6-46ad-,"));

        statusService.report(Verbosity.QUIET);

        assertOutputMatches(".*Last Generated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Objects Mapped: +3 \\(100.0%\\).*");
        assertOutputNotMatches(".*Unmapped Objects: +0 \\(0.0%\\).*");
        assertOutputNotMatches(".*Unknown Objects: +0.*");
        assertOutputMatches(".*Destinations Valid: +No.*");
        assertOutputNotMatches(".*To Default: +2 \\(66.7%\\).*");
        assertOutputMatches(".*Destinations: +2\n.*");
        assertOutputNotMatches(".*Destinations:.*\n +\\* 3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e.*");
        assertOutputNotMatches(".*New Collections: +0\n.*");
    }

    @Test
    public void unmappedObjectsTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        writeCsv(mappingBody("26,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,",
                             "27,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,"));

        statusService.report(Verbosity.NORMAL);

        assertOutputMatches(".*Last Generated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Objects Mapped: +2 \\(66.7%\\).*");
        assertOutputMatches(".*Unmapped Objects: +1 \\(33.3%\\).*");
        assertOutputNotMatches(".*Unmapped Objects:.*\n +\\* 25\n.*");
        assertOutputMatches(".*Unknown Objects: +0.*");
        assertOutputMatches(".*Destinations Valid: +Yes.*");
        assertOutputMatches(".*To Default: +0 \\(0.0%\\).*");
        assertOutputMatches(".*Destinations: +1\n.*");
        assertOutputMatches(".*Destinations:.*\n +\\* 3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e.*");
        assertOutputMatches(".*New Collections: +0\n.*");
    }

    @Test
    public void unmappedObjectsVerboseTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        writeCsv(mappingBody("26,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,",
                             "27,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,"));

        statusService.report(Verbosity.VERBOSE);

        assertOutputMatches(".*Last Generated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Objects Mapped: +2 \\(66.7%\\).*");
        assertOutputMatches(".*Unmapped Objects: +1 \\(33.3%\\).*");
        assertOutputMatches(".*Unmapped Objects:.*\n +\\* 25\n.*");
        assertOutputMatches(".*Unknown Objects: +0.*");
        assertOutputMatches(".*Destinations Valid: +Yes.*");
        assertOutputMatches(".*To Default: +0 \\(0.0%\\).*");
        assertOutputMatches(".*Destinations: +1\n.*");
        assertOutputMatches(".*Destinations:.*\n +\\* 3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e.*");
        assertOutputMatches(".*New Collections: +0\n.*");
    }

    @Test
    public void unknownObjectsTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        writeCsv(mappingBody("25,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,",
                             "26,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,",
                             "27,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,",
                             "55,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,"));

        statusService.report(Verbosity.NORMAL);

        assertOutputMatches(".*Last Generated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Objects Mapped: +3 \\(100.0%\\).*");
        assertOutputMatches(".*Unmapped Objects: +0 \\(0.0%\\).*");
        assertOutputMatches(".*Unknown Objects: +1 .*");
        assertOutputNotMatches(".*Unknown Objects:.*\n +\\* 55\n.*");
        assertOutputMatches(".*Destinations Valid: +Yes.*");
        assertOutputMatches(".*To Default: +0 \\(0.0%\\).*");
        assertOutputMatches(".*Destinations: +1\n.*");
        assertOutputMatches(".*Destinations:.*\n +\\* 3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e.*");
        assertOutputMatches(".*New Collections: +0\n.*");
    }

    @Test
    public void unknownObjectsVerboseTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        writeCsv(mappingBody("25,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,",
                             "26,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,",
                             "27,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,",
                             "55,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,"));

        statusService.report(Verbosity.VERBOSE);

        assertOutputMatches(".*Last Generated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Objects Mapped: +3 \\(100.0%\\).*");
        assertOutputMatches(".*Unmapped Objects: +0 \\(0.0%\\).*");
        assertOutputMatches(".*Unknown Objects: +1 .*");
        assertOutputMatches(".*Unknown Objects:.*\n +\\* 55\n.*");
        assertOutputMatches(".*Destinations Valid: +Yes.*");
        assertOutputMatches(".*To Default: +0 \\(0.0%\\).*");
        assertOutputMatches(".*Destinations: +1\n.*");
        assertOutputMatches(".*Destinations:.*\n +\\* 3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e.*");
        assertOutputMatches(".*New Collections: +0\n.*");
    }

    @Test
    public void allMappedDefaultTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        writeCsv(mappingBody("default,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,001234"));

        statusService.report(Verbosity.NORMAL);

        assertOutputMatches(".*Last Generated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Objects Mapped: +3 \\(100.0%\\).*");
        assertOutputMatches(".*Unmapped Objects: +0 \\(0.0%\\).*");
        assertOutputMatches(".*Unknown Objects: +0.*");
        assertOutputMatches(".*Destinations Valid: +Yes.*");
        assertOutputMatches(".*To Default: +3 \\(100.0%\\).*");
        assertOutputMatches(".*Destinations: +1\n.*");
        assertOutputMatches(".*Destinations:.*\n +\\* 3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e 001234.*");
        assertOutputMatches(".*New Collections: +1\n.*");
        assertOutputMatches(".*New Collections:.*\n +\\* 001234.*");
    }

    @Test
    public void unpopulatedDestObjectsTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        writeCsv(mappingBody("25,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,",
                             "26,,",
                             "27,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,"));

        statusService.report(Verbosity.VERBOSE);

        assertOutputMatches(".*Last Generated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Objects Mapped: +2 \\(66.7%\\).*");
        assertOutputMatches(".*Unmapped Objects: +1 \\(33.3%\\).*");
        assertOutputMatches(".*Unmapped Objects:.*\n +\\* 26\n.*");
        assertOutputMatches(".*Unknown Objects: +0 .*");
        assertOutputMatches(".*Destinations Valid: +No.*");
        assertOutputMatches(".*To Default: +0 \\(0.0%\\).*");
        assertOutputMatches(".*Destinations: +1\n.*");
        assertOutputMatches(".*Destinations:.*\n +\\* 3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e.*");
        assertOutputMatches(".*New Collections: +0\n.*");
    }

    @Test
    public void unpopulatedDestWithDefaultTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        writeCsv(mappingBody("default,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,",
                             "27,,"));

        statusService.report(Verbosity.VERBOSE);

        assertOutputMatches(".*Last Generated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Objects Mapped: +2 \\(66.7%\\).*");
        assertOutputMatches(".*Unmapped Objects: +1 \\(33.3%\\).*");
        assertOutputMatches(".*Unmapped Objects:.*\n +\\* 27\n.*");
        assertOutputMatches(".*Unknown Objects: +0 .*");
        assertOutputMatches(".*Destinations Valid: +No.*");
        assertOutputMatches(".*To Default: +3 \\(100.0%\\).*");
        assertOutputMatches(".*Destinations: +1\n.*");
        assertOutputMatches(".*Destinations:.*\n +\\* 3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e.*");
        assertOutputMatches(".*New Collections: +0\n.*");
    }

    @Test
    public void archivalCollNumsWithPidTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        writeCsv(mappingBody("groupa:group1,bdbd99af-36a5-4bab-9785-e3a802d3737e,"));

        statusService.report(Verbosity.VERBOSE);

        assertOutputMatches(".*Last Generated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Objects Mapped: +0 \\(0.0%\\).*");
        assertOutputMatches(".*Unmapped Objects: +3 \\(100.0%\\).*");
        assertOutputMatches(".*Unmapped Objects:.*\n +\\* 25\n.*");
        assertOutputMatches(".*Unknown Objects: +1 .*");
        assertOutputMatches(".*Destinations Valid: +Yes.*");
        assertOutputMatches(".*To Default: +0 \\(0.0%\\).*");
        assertOutputMatches(".*Destinations: +1\n.*");
        assertOutputMatches(".*Destinations:.*\n +\\* bdbd99af-36a5-4bab-9785-e3a802d3737e.*");
        assertOutputMatches(".*New Collections: +0\n.*");
    }

    @Test
    public void archivalCollNumsNullPidTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        writeCsv(mappingBody("aid:40147,,40147"));

        statusService.report(Verbosity.VERBOSE);

        assertOutputMatches(".*Last Generated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Objects Mapped: +0 \\(0.0%\\).*");
        assertOutputMatches(".*Unmapped Objects: +3 \\(100.0%\\).*");
        assertOutputMatches(".*Unmapped Objects:.*\n +\\* 25\n.*");
        assertOutputMatches(".*Unknown Objects: +0 .*");
        assertOutputMatches(".*Destinations Valid: +No.*");
        assertOutputMatches(".*To Default: +0 \\(0.0%\\).*");
        assertOutputMatches(".*Destinations: +0\n.*");
        assertOutputMatches(".*New Collections: +0\n.*");
    }

    private String mappingBody(String... rows) {
        return String.join(",", DestinationsInfo.CSV_HEADERS) + "\n"
                + String.join("\n", rows);
    }

    private void writeCsv(String mappingBody) throws IOException {
        FileUtils.write(project.getDestinationMappingsPath().toFile(),
                mappingBody, StandardCharsets.UTF_8);
        project.getProjectProperties().setDestinationsGeneratedDate(Instant.now());
        ProjectPropertiesSerialization.write(project);
    }
}