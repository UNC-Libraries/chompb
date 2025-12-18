package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo;
import edu.unc.lib.boxc.migration.cdm.test.BxcEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.SipServiceHelper;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;

import static edu.unc.lib.boxc.migration.cdm.test.PostMigrationReportTestHelper.assertContainsRow;
import static edu.unc.lib.boxc.migration.cdm.test.PostMigrationReportTestHelper.parseReport;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.MockitoAnnotations.openMocks;

/**
 * @author bbpennel
 */
public class PostMigrationReportServiceTest {
    private static final String BOXC_BASE_URL = "http://localhost:46887/bxc/record/";
    private static final String BOXC_ID_1 = "bb3b83d7-2962-4604-a7d0-9afcb4ec99b1";
    private static final String BOXC_ID_2 = "91c08272-260f-40f1-bb7c-78854d504368";
    private static final String BOXC_ID_3 = "f9d7262c-3cfb-4d27-8ecc-b9df9ac2f950";
    private static final String BOXC_URL_1 = BOXC_BASE_URL + BOXC_ID_1;
    private static final String BOXC_URL_2 = BOXC_BASE_URL + BOXC_ID_2;
    private static final String BOXC_URL_3 = BOXC_BASE_URL + BOXC_ID_3;
    private AutoCloseable closeable;
    @TempDir
    public Path tmpFolder;

    private MigrationProject project;
    private SipServiceHelper testHelper;
    private DescriptionsService descriptionsService;
    private PostMigrationReportService service;

    public void setup(String cdmEnvId, String projectSource) throws Exception {
        closeable = openMocks(this);
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder, "proj", null, "user", cdmEnvId,
                BxcEnvironmentHelper.DEFAULT_ENV_ID, projectSource);
        testHelper = new SipServiceHelper(project, tmpFolder);
        descriptionsService = testHelper.getDescriptionsService();

        service = new PostMigrationReportService();
        service.setProject(project);
        service.setChompbConfig(testHelper.getChompbConfig());
        service.setDescriptionsService(testHelper.getDescriptionsService());
        service.setSourceFileService(testHelper.getSourceFileService());
        service.init();
    }

    @AfterEach
    void closeService() throws Exception {
        closeable.close();
    }

    @Test
    public void addSingleItemTest() throws Exception {
        setup(CdmEnvironmentHelper.DEFAULT_ENV_ID, MigrationProject.PROJECT_SOURCE_CDM);
        testHelper.indexExportData("mini_gilmer");
        Path srcPath1 = testHelper.addSourceFile("25.txt");
        writeSourceFileCsv(mappingBody("25,," + srcPath1 +","));
        testHelper.populateDescriptions("gilmer_mods1.xml");

        service.addWorkRow("25", BOXC_ID_1, 1, true);
        service.addFileRow("25/original_file", "25", BOXC_ID_1, BOXC_ID_2, true);
        service.closeCsv();

        var rows = parseReport(project);
        assertContainsRow(rows, "25",
                "http://localhost/cdm/singleitem/collection/proj/id/25",
                "Work",
                BOXC_URL_1,
                "Redoubt C",
                "",
                srcPath1.toString(),
                "",
                "",
                "",
                "1",
                "",
                "");
        assertContainsRow(rows, "25/original_file",
                "http://localhost/cdm/singleitem/collection/proj/id/25",
                "File",
                BOXC_URL_2,
                "",
                "",
                srcPath1.toString(),
                "",
                BOXC_URL_1,
                "Redoubt C",
                "",
                "",
                "");
    }

    @Test
    public void addSingleItemWithFileDescTest() throws Exception {
        setup(CdmEnvironmentHelper.DEFAULT_ENV_ID, MigrationProject.PROJECT_SOURCE_CDM);
        testHelper.indexExportData("mini_gilmer");
        Path srcPath1 = testHelper.addSourceFile("25.txt");
        writeSourceFileCsv(mappingBody("25,," + srcPath1 +","));
        testHelper.populateDescriptions("gilmer_mods1.xml", "gilmer_mods_children.xml");

        service.addWorkRow("25", BOXC_ID_1, 1, true);
        service.addFileRow("25/original_file", "25", BOXC_ID_1, BOXC_ID_2, true);
        service.closeCsv();

        var rows = parseReport(project);
        assertContainsRow(rows, "25",
                "http://localhost/cdm/singleitem/collection/proj/id/25",
                "Work",
                BOXC_URL_1,
                "Redoubt C",
                "",
                srcPath1.toString(),
                "",
                "",
                "",
                "1",
                "",
                "");
        assertContainsRow(rows, "25/original_file",
                "http://localhost/cdm/singleitem/collection/proj/id/25",
                "File",
                BOXC_URL_2,
                "Redoubt C Scan File",
                "",
                srcPath1.toString(),
                "",
                BOXC_URL_1,
                "Redoubt C",
                "",
                "",
                "");
    }

    @Test
    public void addGroupedTest() throws Exception {
        setup(CdmEnvironmentHelper.DEFAULT_ENV_ID, MigrationProject.PROJECT_SOURCE_CDM);
        testHelper.indexExportData("grouped_gilmer");
        Path srcPath1 = testHelper.addSourceFile("26.txt");
        Path srcPath2 = testHelper.addSourceFile("27.txt");
        writeSourceFileCsv(mappingBody("26,," + srcPath1 +",", "27,," + srcPath2 +","));
        testHelper.populateDescriptions("grouped_mods.xml");

        service.addWorkRow("grp:groupa:group1", BOXC_ID_1, 2, false);
        service.addFileRow("26", "grp:groupa:group1", BOXC_ID_1, BOXC_ID_2, false);
        service.addFileRow("27", "grp:groupa:group1", BOXC_ID_1, BOXC_ID_3, false);
        service.closeCsv();

        var rows = parseReport(project);
        assertContainsRow(rows, "grp:groupa:group1",
                "",
                "Work",
                BOXC_URL_1,
                "Folder Group 1",
                "",
                "",
                "",
                "",
                "",
                "2",
                "",
                "");
        assertContainsRow(rows, "26",
                "http://localhost/cdm/singleitem/collection/proj/id/26",
                "File",
                BOXC_URL_2,
                "Plan of Battery McIntosh",
                "",
                srcPath1.toString(),
                "",
                BOXC_URL_1,
                "Folder Group 1",
                "",
                "",
                "");
        assertContainsRow(rows, "27",
                "http://localhost/cdm/singleitem/collection/proj/id/27",
                "File",
                BOXC_URL_3,
                "Fort DeRussy on Red River, Louisiana",
                "",
                srcPath2.toString(),
                "",
                BOXC_URL_1,
                "Folder Group 1",
                "",
                "",
                "");
    }

    @Test
    public void addCompoundTest() throws Exception {
        setup(CdmEnvironmentHelper.DEFAULT_ENV_ID, MigrationProject.PROJECT_SOURCE_CDM);
        testHelper.indexExportData(Paths.get("src/test/resources/keepsakes_fields.csv"), "mini_keepsakes");
        Path srcPath1 = testHelper.addSourceFile("nccg_ck_1042-22_v1.tif");
        Path srcPath2 = testHelper.addSourceFile("nccg_ck_1042-22_v2.tif");
        writeSourceFileCsv(mappingBody("602,," + srcPath1 +",", "603,," + srcPath2 +","));
        descriptionsService.generateDocuments(true);
        descriptionsService.expandDescriptions();

        service.addWorkRow("605", BOXC_ID_1, 2, false);
        service.addFileRow("602", "605", BOXC_ID_1, BOXC_ID_2, false);
        service.addFileRow("603", "605", BOXC_ID_1, BOXC_ID_3, false);
        service.closeCsv();

        var rows = parseReport(project);
        assertContainsRow(rows, "605",
                "http://localhost/cdm/compoundobject/collection/proj/id/605",
                "Work",
                BOXC_URL_1,
                "Tiffany's pillbox commemorating UNC's bicentennial (closed, in box)",
                "",
                "",
                "",
                "",
                "",
                "2",
                "",
                "");
        assertContainsRow(rows, "602",
                "http://localhost/cdm/singleitem/collection/proj/id/602",
                "File",
                BOXC_URL_2,
                "World War II ration book",
                "",
                srcPath1.toString(),
                "",
                BOXC_URL_1,
                "Tiffany's pillbox commemorating UNC's bicentennial (closed, in box)",
                "",
                "",
                "");
        assertContainsRow(rows, "603",
                "http://localhost/cdm/singleitem/collection/proj/id/603",
                "File",
                BOXC_URL_3,
                "World War II ration book (instructions)",
                "",
                srcPath2.toString(),
                "",
                BOXC_URL_1,
                "Tiffany's pillbox commemorating UNC's bicentennial (closed, in box)",
                "",
                "",
                "");
    }

    @Test
    public void addSingleItemNonCdmProjectTest() throws Exception {
        setup(null, MigrationProject.PROJECT_SOURCE_FILES);
        project.getProjectProperties().setCdmEnvironmentId(null);
        project.getProjectProperties().setProjectSource(MigrationProject.PROJECT_SOURCE_FILES);
        ProjectPropertiesSerialization.write(project);
        project = MigrationProjectFactory.loadMigrationProject(project.getProjectPath());

        testHelper.indexExportData("mini_gilmer");
        Path srcPath1 = testHelper.addSourceFile("25.txt");
        writeSourceFileCsv(mappingBody("25,," + srcPath1 +","));
        testHelper.populateDescriptions("gilmer_mods1.xml");

        service.addWorkRow("25", BOXC_ID_1, 1, true);
        service.addFileRow("25/original_file", "25", BOXC_ID_1, BOXC_ID_2, true);
        service.closeCsv();

        assertFalse(Files.exists(project.getPostMigrationReportPath()));
    }

    private String mappingBody(String... rows) {
        return String.join(",", SourceFilesInfo.CSV_HEADERS) + "\n"
                + String.join("\n", rows);
    }

    private void writeSourceFileCsv(String mappingBody) throws IOException {
        FileUtils.write(project.getSourceFilesMappingPath().toFile(),
                mappingBody, StandardCharsets.UTF_8);
        project.getProjectProperties().setSourceFilesUpdatedDate(Instant.now());
        ProjectPropertiesSerialization.write(project);
    }
}
