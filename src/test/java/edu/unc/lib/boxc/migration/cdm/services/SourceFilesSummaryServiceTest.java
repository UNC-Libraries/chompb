package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.AbstractOutputTest;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo;
import edu.unc.lib.boxc.migration.cdm.options.Verbosity;
import edu.unc.lib.boxc.migration.cdm.status.SourceFilesSummaryService;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.SipServiceHelper;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

public class SourceFilesSummaryServiceTest extends AbstractOutputTest {
    private static final String PROJECT_NAME = "proj";
    private static final String USERNAME = "migr_user";

    @TempDir
    public Path tmpFolder;

    private MigrationProject project;
    private SipServiceHelper testHelper;
    private SourceFileService sourceFileService;
    private SourceFilesSummaryService summaryService;

    private Path basePath;

    @BeforeEach
    public void setup() throws Exception {
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder, PROJECT_NAME, null, USERNAME, CdmEnvironmentHelper.DEFAULT_ENV_ID);

        basePath = tmpFolder.resolve("testFolder");
        Files.createDirectory(basePath);

        testHelper = new SipServiceHelper(project, basePath);
        sourceFileService = new SourceFileService();
        sourceFileService.setProject(project);
        summaryService = new SourceFilesSummaryService();
        summaryService.setProject(project);
        summaryService.setSourceFileService(sourceFileService);
    }

    @Test
    public void summaryOutputTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        Path path1 = testHelper.addSourceFile("25.txt");
        writeCsv(mappingBody("25,," + path1 +","));

        summaryService.summary(Verbosity.NORMAL);

        assertOutputMatches(".*New Files Mapped: +1.*");
        assertOutputMatches(".*Total Files Mapped: +1.*");
        assertOutputMatches(".*Total Files in Project: +3.*");
    }

    @Test
    public void summaryDuplicateEntriesTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        Path path1 = testHelper.addSourceFile("25.txt");
        writeCsv(mappingBody("25,," + path1 +",", "25,," + path1 +","));

        summaryService.summary(Verbosity.NORMAL);

        assertOutputMatches(".*New Files Mapped: +1.*");
        assertOutputMatches(".*Total Files Mapped: +1.*");
        assertOutputMatches(".*Total Files in Project: +3.*");
    }

    @Test
    public void summaryIdsNotInIndexTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        Path path1 = testHelper.addSourceFile("25.txt");
        writeCsv(mappingBody("2,," + path1 +","));

        summaryService.summary(Verbosity.NORMAL);

        assertOutputMatches(".*New Files Mapped: +0.*");
        assertOutputMatches(".*Total Files Mapped: +0.*");
        assertOutputMatches(".*Total Files in Project: +3.*");
    }

    @Test
    public void summaryNothingMappedTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");

        summaryService.summary(Verbosity.NORMAL);

        assertOutputMatches(".*New Files Mapped: +0.*");
        assertOutputMatches(".*Total Files Mapped: +0.*");
        assertOutputMatches(".*Total Files in Project: +3.*");
    }

    @Test
    public void summaryEverythingMapped() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        Path path1 = testHelper.addSourceFile("25.txt");
        Path path2 = testHelper.addSourceFile("26.txt");
        Path path3 = testHelper.addSourceFile("27.txt");
        writeCsv(mappingBody("25,," + path1 +",", "26,," + path2 +",", "27,," + path3 +","));

        summaryService.summary(Verbosity.NORMAL);

        assertOutputMatches(".*New Files Mapped: +3.*");
        assertOutputMatches(".*Total Files Mapped: +3.*");
        assertOutputMatches(".*Total Files in Project: +3.*");
    }

    @Test
    public void summaryNoPathMapped() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        Path path1 = testHelper.addSourceFile("25.txt");
        writeCsv(mappingBody("25,," + path1 +",", "26,,,"));

        summaryService.summary(Verbosity.NORMAL);

        assertOutputMatches(".*New Files Mapped: +1.*");
        assertOutputMatches(".*Total Files Mapped: +1.*");
        assertOutputMatches(".*Total Files in Project: +3.*");
    }

    @Test
    public void summaryDryRun() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        Path path1 = testHelper.addSourceFile("25.txt");
        writeTempCsv(mappingBody("25,," + path1 +","));
        summaryService.setDryRun(true);

        summaryService.summary(Verbosity.NORMAL);

        assertOutputMatches(".*New Files Mapped: +1.*");
        assertOutputMatches(".*Total Files Mapped: +1.*");
        assertOutputMatches(".*Total Files in Project: +3.*");
    }

    @Test
    public void summaryDryRunUpdateAddSourceFile() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        Path path1 = testHelper.addSourceFile("25.txt");
        Path path2 = testHelper.addSourceFile("26.txt");
        writeCsv(mappingBody("25,," + path1 +","));
        writeTempCsv(mappingBody("26,," + path2 +","));
        summaryService.setDryRun(true);

        summaryService.summary(Verbosity.NORMAL);

        assertOutputMatches(".*New Files Mapped: +1.*");
        assertOutputMatches(".*Total Files Mapped: +2.*");
        assertOutputMatches(".*Total Files in Project: +3.*");
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

    private void writeTempCsv(String mappingBody) throws IOException {
        Path mappingPath = project.getSourceFilesMappingPath();
        Path tempMappingPath = mappingPath.getParent().resolve("~" + mappingPath.getFileName().toString() + "_new");
        FileUtils.write(tempMappingPath.toFile(),
                mappingBody, StandardCharsets.UTF_8);
        project.getProjectProperties().setSourceFilesUpdatedDate(Instant.now());
        ProjectPropertiesSerialization.write(project);
    }
}
