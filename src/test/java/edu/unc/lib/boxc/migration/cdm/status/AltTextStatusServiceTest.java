package edu.unc.lib.boxc.migration.cdm.status;

import edu.unc.lib.boxc.migration.cdm.AbstractOutputTest;
import edu.unc.lib.boxc.migration.cdm.model.AltTextInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.Verbosity;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.test.BxcEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.SipServiceHelper;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;

public class AltTextStatusServiceTest extends AbstractOutputTest {
    private static final String PROJECT_NAME = "proj";
    private static final String USERNAME = "migr_user";

    @TempDir
    public Path tmpFolder;

    private MigrationProject project;
    private SipServiceHelper testHelper;
    private AltTextStatusService statusService;

    @BeforeEach
    public void setup() throws Exception {
        project = MigrationProjectFactory.createCdmMigrationProject(
                tmpFolder, PROJECT_NAME, null, USERNAME,
                CdmEnvironmentHelper.DEFAULT_ENV_ID, BxcEnvironmentHelper.DEFAULT_ENV_ID);

        testHelper = new SipServiceHelper(project, tmpFolder);
        statusService = new AltTextStatusService();
        statusService.setProject(project);
        statusService.setIndexService(testHelper.getCdmIndexService());
    }

    @Test
    public void mappingsNotGeneratedTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        statusService.report(Verbosity.NORMAL);

        assertOutputMatches(".*Last Updated: +Not completed.*");
    }

    @Test
    public void unlistedObjectsTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        writeCsv(mappingBody("25,alt text"));

        statusService.report(Verbosity.NORMAL);

        assertOutputMatches(".*Last Updated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Objects Mapped: +1 \\(33.3%\\).*");
        assertOutputMatches(".*Unknown Objects: +0.*");
        assertOutputMatches(".*Unmapped Objects: +2 \\(66.7%\\).*");
        assertOutputNotMatches(".*Unmapped Objects:.*\n + \\* 26.*");
        assertOutputMatches(".*Mappings Valid: +Yes.*");
    }

    @Test
    public void unlistedObjectsVerboseTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        writeCsv(mappingBody("25,alt text"));

        statusService.report(Verbosity.VERBOSE);

        assertOutputMatches(".*Last Updated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Objects Mapped: +1 \\(33.3%\\).*");
        assertOutputMatches(".*Unknown Objects: +0.*");
        assertOutputMatches(".*Unmapped Objects: +2 \\(66.7%\\).*");
        assertOutputMatches(".*Unmapped Objects:.* + \\* 26.*");
        assertOutputMatches(".*Unmapped Objects:.* + \\* 27.*");
        assertOutputMatches(".*Mappings Valid: +Yes.*");
    }

    @Test
    public void blankAltTextBodyTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        writeCsv(mappingBody("25,alt text",
                "26,",
                "27,"));

        statusService.report(Verbosity.VERBOSE);

        assertOutputMatches(".*Last Updated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Objects Mapped: +1 \\(33.3%\\).*");
        assertOutputMatches(".*Unknown Objects: +0.*");
        assertOutputMatches(".*Unmapped Objects: +2 \\(66.7%\\).*");
        assertOutputMatches(".*Unmapped Objects:.* + \\* 26.*");
        assertOutputMatches(".*Unmapped Objects:.* + \\* 27.*");
        assertOutputMatches(".*Mappings Valid: +Yes.*");
    }

    @Test
    public void unknownIdTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        writeCsv(mappingBody("25,alt text",
                "55,alt text",
                "27,more alt text"));

        statusService.report(Verbosity.NORMAL);

        assertOutputMatches(".*Last Updated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Objects Mapped: +2 \\(66.7%\\).*");
        assertOutputMatches(".*Unmapped Objects: +1 \\(33.3%\\).*");
        assertOutputMatches(".*Unknown Objects: +1.*");
        assertOutputMatches(".*Mappings Valid: +Yes.*");
    }

    @Test
    public void unknownIdVerboseTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        writeCsv(mappingBody("25,alt text",
                "55,alt text",
                "27,more alt text"));

        statusService.report(Verbosity.VERBOSE);

        assertOutputMatches(".*Last Updated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Objects Mapped: +2 \\(66.7%\\).*");
        assertOutputMatches(".*Unmapped Objects: +1 \\(33.3%\\).*");
        assertOutputMatches(".*Unknown Objects: +1.*");
        assertOutputMatches(".*Unknown Objects:.* + \\* 55.*");
        assertOutputMatches(".*Mappings Valid: +Yes.*");
    }

    private String mappingBody(String... rows) {
        return String.join(",", AltTextInfo.CSV_HEADERS) + "\n"
                + String.join("\n", rows);
    }

    private void writeCsv(String mappingBody) throws IOException {
        FileUtils.write(project.getAltTextMappingPath().toFile(),
                mappingBody, StandardCharsets.UTF_8);
        project.getProjectProperties().setAltTextFilesUpdatedDate(Instant.now());
        ProjectPropertiesSerialization.write(project);
    }
}
