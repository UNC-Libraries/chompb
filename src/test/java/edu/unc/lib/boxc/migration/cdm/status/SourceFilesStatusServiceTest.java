package edu.unc.lib.boxc.migration.cdm.status;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;

import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import edu.unc.lib.boxc.migration.cdm.AbstractOutputTest;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo;
import edu.unc.lib.boxc.migration.cdm.options.Verbosity;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.test.SipServiceHelper;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;

/**
 * @author bbpennel
 */
public class SourceFilesStatusServiceTest extends AbstractOutputTest {
    private static final String PROJECT_NAME = "proj";
    private static final String USERNAME = "migr_user";

    @TempDir
    public Path tmpFolder;

    private MigrationProject project;
    private SipServiceHelper testHelper;
    private SourceFilesStatusService statusService;

    @BeforeEach
    public void setup() throws Exception {
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder, PROJECT_NAME, null, USERNAME, CdmEnvironmentHelper.DEFAULT_ENV_ID);

        testHelper = new SipServiceHelper(project, tmpFolder);
        statusService = new SourceFilesStatusService();
        statusService.setProject(project);
        statusService.setStreamingMetadataService(testHelper.getStreamingMetadataService());
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
        Path path1 = testHelper.addSourceFile("25.txt");
        writeCsv(mappingBody("25,," + path1 +","));

        statusService.report(Verbosity.NORMAL);

        assertOutputMatches(".*Last Updated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Objects Mapped: +1 \\(33.3%\\).*");
        assertOutputMatches(".*Unknown Objects: +0.*");
        assertOutputMatches(".*Unmapped Objects: +2 \\(66.7%\\).*");
        assertOutputNotMatches(".*Unmapped Objects:.*\n + \\* 26.*");
        assertOutputMatches(".*Mappings Valid: +Yes.*");
        assertOutputMatches(".*Potential Matches: +0.*");
    }

    @Test
    public void unlistedObjectsVerboseTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        Path path1 = testHelper.addSourceFile("25.txt");
        writeCsv(mappingBody("25,," + path1 +","));

        statusService.report(Verbosity.VERBOSE);

        assertOutputMatches(".*Last Updated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Objects Mapped: +1 \\(33.3%\\).*");
        assertOutputMatches(".*Unknown Objects: +0.*");
        assertOutputMatches(".*Unmapped Objects: +2 \\(66.7%\\).*");
        assertOutputMatches(".*Unmapped Objects:.* + \\* 26.*");
        assertOutputMatches(".*Unmapped Objects:.* + \\* 27.*");
        assertOutputMatches(".*Mappings Valid: +Yes.*");
        assertOutputMatches(".*Potential Matches: +0.*");
    }

    @Test
    public void blankPathsTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        Path path1 = testHelper.addSourceFile("25.txt");
        writeCsv(mappingBody("25,," + path1 +",",
                             "26,,,",
                             "27,,,"));

        statusService.report(Verbosity.VERBOSE);

        assertOutputMatches(".*Last Updated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Objects Mapped: +1 \\(33.3%\\).*");
        assertOutputMatches(".*Unknown Objects: +0.*");
        assertOutputMatches(".*Unmapped Objects: +2 \\(66.7%\\).*");
        assertOutputMatches(".*Unmapped Objects:.* + \\* 26.*");
        assertOutputMatches(".*Unmapped Objects:.* + \\* 27.*");
        assertOutputMatches(".*Mappings Valid: +Yes.*");
        assertOutputMatches(".*Potential Matches: +0.*");
    }

    @Test
    public void potentialMatchesTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        Path path1 = testHelper.addSourceFile("25.txt");
        Path path3 = testHelper.addSourceFile("27.txt");
        writeCsv(mappingBody("25,," + path1 +",",
                             "26,,,/path/to/potential.txt",
                             "27,," + path3 + ",/path/to/potential2.txt"));

        statusService.report(Verbosity.NORMAL);

        assertOutputMatches(".*Last Updated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Objects Mapped: +2 \\(66.7%\\).*");
        assertOutputMatches(".*Unmapped Objects: +1 \\(33.3%\\).*");
        assertOutputMatches(".*Unknown Objects: +0.*");
        assertOutputMatches(".*Mappings Valid: +Yes.*");
        assertOutputMatches(".*Potential Matches: +1.*");
    }

    @Test
    public void unknownIdTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        Path path1 = testHelper.addSourceFile("25.txt");
        Path path2 = testHelper.addSourceFile("26.txt");
        Path path3 = testHelper.addSourceFile("27.txt");
        writeCsv(mappingBody("25,," + path1 +",",
                             "55,," + path2 + ",",
                             "27,," + path3 + ","));

        statusService.report(Verbosity.NORMAL);

        assertOutputMatches(".*Last Updated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Objects Mapped: +2 \\(66.7%\\).*");
        assertOutputMatches(".*Unmapped Objects: +1 \\(33.3%\\).*");
        assertOutputMatches(".*Unknown Objects: +1.*");
        assertOutputMatches(".*Mappings Valid: +Yes.*");
        assertOutputMatches(".*Potential Matches: +0.*");
    }

    @Test
    public void unknownIdVerboseTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        Path path1 = testHelper.addSourceFile("25.txt");
        Path path2 = testHelper.addSourceFile("26.txt");
        Path path3 = testHelper.addSourceFile("27.txt");
        writeCsv(mappingBody("25,," + path1 +",",
                             "55,," + path2 + ",",
                             "27,," + path3 + ","));

        statusService.report(Verbosity.VERBOSE);

        assertOutputMatches(".*Last Updated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Objects Mapped: +2 \\(66.7%\\).*");
        assertOutputMatches(".*Unmapped Objects: +1 \\(33.3%\\).*");
        assertOutputMatches(".*Unknown Objects: +1.*");
        assertOutputMatches(".*Unknown Objects:.* + \\* 55.*");
        assertOutputMatches(".*Mappings Valid: +Yes.*");
        assertOutputMatches(".*Potential Matches: +0.*");
    }

    private String mappingBody(String... rows) {
        return String.join(",", SourceFilesInfo.CSV_HEADERS) + "\n"
                + String.join("\n", rows);
    }

    private void writeCsv(String mappingBody) throws IOException {
        FileUtils.write(project.getSourceFilesMappingPath().toFile(),
                mappingBody, StandardCharsets.UTF_8);
        project.getProjectProperties().setSourceFilesUpdatedDate(Instant.now());
        ProjectPropertiesSerialization.write(project);
    }
}
