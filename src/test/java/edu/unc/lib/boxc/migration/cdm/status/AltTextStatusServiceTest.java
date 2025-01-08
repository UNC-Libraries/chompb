package edu.unc.lib.boxc.migration.cdm.status;

import edu.unc.lib.boxc.migration.cdm.AbstractOutputTest;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo;
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
    }

    @Test
    public void noAltTextObjectsTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        Path csvPath = writeCsv(mappingBody("25,alt-text"));

        statusService.report(csvPath, Verbosity.NORMAL);

        assertOutputMatches(".*Last Updated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Objects with Alt-text: +1 \\(33.3%\\).*");
        assertOutputMatches(".*Objects without Alt-text: +2 \\(66.7%\\).*");
        assertOutputNotMatches(".*Objects without Alt-text:.*\n + \\* 26.*");
        assertOutputMatches(".*Unknown Objects: +0.*");
        assertOutputMatches(".*Alt-text Upload File Valid: +Yes.*");
    }

    @Test
    public void noAltTextObjectsVerboseTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        Path csvPath = writeCsv(mappingBody("25,alt-text"));

        statusService.report(csvPath, Verbosity.VERBOSE);

        assertOutputMatches(".*Last Updated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Objects with Alt-text: +1 \\(33.3%\\).*");
        assertOutputMatches(".*Unknown Objects: +0.*");
        assertOutputMatches(".*Objects without Alt-text: +2 \\(66.7%\\).*");
        assertOutputMatches(".*Objects without Alt-text:.*\n + \\* 26.*");
        assertOutputMatches(".*Objects without Alt-text:.*\n + \\* 27.*");
        assertOutputMatches(".*Alt-text Upload File Valid: +Yes.*");
    }

    @Test
    public void blankAltTextTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        Path csvPath = writeCsv(mappingBody("25,alt-text", "26,"));

        statusService.report(csvPath, Verbosity.VERBOSE);

        assertOutputMatches(".*Last Updated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Objects with Alt-text: +1 \\(33.3%\\).*");
        assertOutputMatches(".*Unknown Objects: +0.*");
        assertOutputMatches(".*Objects without Alt-text: +2 \\(66.7%\\).*");
        assertOutputMatches(".*Objects without Alt-text:.*\n + \\* 26.*");
        assertOutputMatches(".*Objects without Alt-text:.*\n + \\* 27.*");
        assertOutputMatches(".*Alt-text Upload File Valid: +No.*");
    }

    @Test
    public void unknownIdTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        Path csvPath = writeCsv(mappingBody("25,alt-text",
                "55,more alt-text",
                "27, even more alt-text"));

        statusService.report(csvPath, Verbosity.NORMAL);

        assertOutputMatches(".*Last Updated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Objects with Alt-text: +2 \\(66.7%\\).*");
        assertOutputMatches(".*Objects without Alt-text: +1 \\(33.3%\\).*");
        assertOutputMatches(".*Unknown Objects: +1.*");
        assertOutputMatches(".*Alt-text Upload File Valid: +Yes.*");
    }

    @Test
    public void unknownIdVerboseTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        Path csvPath = writeCsv(mappingBody("25,alt-text",
                "55,more alt-text",
                "27,even more alt-text"));

        statusService.report(csvPath, Verbosity.VERBOSE);

        assertOutputMatches(".*Last Updated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Objects with Alt-text: +2 \\(66.7%\\).*");
        assertOutputMatches(".*Objects without Alt-text: +1 \\(33.3%\\).*");
        assertOutputMatches(".*Unknown Objects: +1.*");
        assertOutputMatches(".*Unknown Objects:.* + \\* 55.*");
        assertOutputMatches(".*Alt-text Upload File Valid: +Yes.*");
    }

    private String mappingBody(String... rows) {
        return String.join(",", SourceFilesInfo.CSV_HEADERS) + "\n"
                + String.join("\n", rows);
    }

    private Path writeCsv(String mappingBody) throws IOException {
        Path csvPath = project.getAltTextCsvPath();
        FileUtils.write(csvPath.toFile(),
                mappingBody, StandardCharsets.UTF_8);
        ProjectPropertiesSerialization.write(project);
        return csvPath;
    }
}
