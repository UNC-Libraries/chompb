package edu.unc.lib.boxc.migration.cdm;

import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo.CdmFieldEntry;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.CdmFieldService;
import edu.unc.lib.boxc.migration.cdm.services.CdmFileRetrievalService;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.test.BxcEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.TestSshServer;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmHeaderConstants.STANDARDIZED_COLLECTION_NAME;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmHeaderConstants.STANDARDIZED_CONTAINER_TYPE;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmHeaderConstants.STANDARDIZED_GEOGRAPHIC_NAME;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmHeaderConstants.TSV_STANDARDIZED_HEADERS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author bbpennel
 */
@WireMockTest(httpPort = BxcEnvironmentHelper.TEST_HTTP_PORT)
public class CdmExportCommandIT extends AbstractCommandIT {

    private final static String COLLECTION_ID = "gilmer";

    private TestSshServer testSshServer;

    private CdmFieldService fieldService;

    @BeforeEach
    public void setUp() throws Exception {
        fieldService = new CdmFieldService();
        testSshServer = new TestSshServer();
        testSshServer.startServer();
        setupChompbConfig();
    }

    @AfterEach
    public void cleanup() throws Exception {
        testSshServer.stopServer();
    }

    private String[] exportArgs(Path projPath, String... extras) {
        String[] defaultArgs = new String[] {
                "-w", projPath.toString(),
                "--env-config", chompbConfigPath,
                "export",
                "-p", TestSshServer.PASSWORD};
        return ArrayUtils.addAll(defaultArgs, extras);
    }

    @Test
    public void exportValidProjectTest() throws Exception {
        Path projPath = createProject();

        String[] args = exportArgs(projPath);
        executeExpectSuccess(args);

        MigrationProject project = MigrationProjectFactory.loadMigrationProject(projPath);

        assertTrue(Files.exists(project.getExportPath()), "Export folder not created");
        assertDescAllFilePresent(project, "/descriptions/gilmer/index/description/desc.all");
    }

    @Test
    public void noUsernameTest() throws Exception {
        System.clearProperty("user.name");
        Path projPath = createProject();

        String[] args = exportArgs(projPath);
        executeExpectFailure(args);

        MigrationProject project = MigrationProjectFactory.loadMigrationProject(projPath);
        assertFalse(Files.exists(project.getExportPath()), "Description folder should not be created");
        assertOutputContains("Must provided a CDM username");
    }

    @Test
    public void noPasswordTest() throws Exception {
        Path projPath = createProject();

        String[] args = new String[] {
                "-w", projPath.toString(),
                "export" };
        executeExpectFailure(args);

        MigrationProject project = MigrationProjectFactory.loadMigrationProject(projPath);
        assertFalse(Files.exists(project.getExportPath()), "Description folder should not be created");
        assertOutputContains("Must provided a CDM password");
    }

    @Test
    public void errorResponseTest() throws Exception {
        Path projPath = createProject("bad_colletion");

        String[] args = exportArgs(projPath);
        executeExpectFailure(args);

        MigrationProject project = MigrationProjectFactory.loadMigrationProject(projPath);

        assertFalse(Files.exists(CdmFileRetrievalService.getDescAllPath(project)), "Export file should not be created");
        assertOutputContains("Failed to download desc.all file");
    }

    @Test
    public void rerunCompletedExportTest() throws Exception {
        Path projPath = createProject();

        String[] args = exportArgs(projPath);
        executeExpectSuccess(args);

        String[] argsRerun = exportArgs(projPath);
        executeExpectFailure(argsRerun);
        assertOutputContains("Export has already completed, must force restart to overwrite");

        MigrationProject project = MigrationProjectFactory.loadMigrationProject(projPath);

        // Previous export should still be present
        assertDescAllFilePresent(project, "/descriptions/gilmer/index/description/desc.all");

        // Remove file so we can see that it gets repopulated
        Files.delete(CdmFileRetrievalService.getDescAllPath(project));

        // Retry with force restart
        String[] argsRestart = exportArgs(projPath, "--force");
        executeExpectSuccess(argsRestart);

        // Contents of file should match new contents
        assertDescAllFilePresent(project, "/descriptions/gilmer/index/description/desc.all");
    }

    @Test
    public void exportValidProjectWithCompoundsTest() throws Exception {
        Path projPath = createProject("mini_keepsakes");

        String[] args = exportArgs(projPath);
        executeExpectSuccess(args);

        MigrationProject project = MigrationProjectFactory.loadMigrationProject(projPath);

        assertTrue(Files.exists(project.getExportPath()), "Export folder not created");
        assertDescAllFilePresent(project, "/descriptions/mini_keepsakes/index/description/desc.all");

        assertCpdFilePresent(project, "617.cpd", "/descriptions/mini_keepsakes/image/617.cpd");
        assertCpdFilePresent(project, "620.cpd", "/descriptions/mini_keepsakes/image/620.cpd");
    }

    @Test
    public void exportValidProjectWithMonographCompoundsTest() throws Exception {
        Path projPath = createProject("monograph");

        String[] args = exportArgs(projPath);
        executeExpectSuccess(args);

        MigrationProject project = MigrationProjectFactory.loadMigrationProject(projPath);

        assertTrue(Files.exists(project.getExportPath()), "Export folder not created");
        assertDescAllFilePresent(project, "/descriptions/monograph/index/description/desc.all");

        assertCpdFilePresent(project, "196.cpd", "/descriptions/monograph/image/196.cpd");
    }

    @Test
    public void exportValidProjectWithPdfCompoundsTest() throws Exception {
        Path projPath = createProject("pdf");

        String[] args = exportArgs(projPath);
        executeExpectSuccess(args);

        MigrationProject project = MigrationProjectFactory.loadMigrationProject(projPath);

        assertTrue(Files.exists(project.getExportPath()), "Export folder not created");
        assertDescAllFilePresent(project, "/descriptions/pdf/index/description/desc.all");

        assertCpdFilePresent(project, "17941.cpd", "/descriptions/pdf/image/17941.cpd");
    }

    @Test
    public void exportEadToCdmTest() throws Exception {
        eadToCdmApiResponse("04428", "{\"04428\":[{\"collection_name\":\"Joyner Family Papers, ; 4428\",\"collection_number\":\"04428\"," +
                "\"location_in_collection\":\"Series 1. Correspondence, 1836-1881.\",\"citation\":\"[Identification of item], " +
                "in the Joyner Family Papers #4428, Southern Historical Collection, Wilson Special Collections Library, University " +
                "of North Carolina at Chapel Hill.\",\"container_type\":\"Folder\",\"hook_id\":\"folder_1\",\"object\":\"Folder 1: " +
                "April 1836-15 October 1858, (17 items): Scan 1\",\"collection_url\":\"https:\\/\\/finding-aids.lib.unc.edu\\/catalog\\/04428\"," +
                "\"genre_form\":\"\",\"extent\":\"\",\"unit_date\":\"\",\"geographic_name\":\"\",\"processinfo\":\"\",\"scopecontent\":\"\"," +
                "\"unit_title\":\"April 1836-15 October 1858, (17 items)\",\"container\":\"1\"}]}");
        Path projPath = createProject("04428_ead");
        String[] args = exportArgs(projPath, "-ead");
        executeExpectSuccess(args);

        MigrationProject project = MigrationProjectFactory.loadMigrationProject(projPath);
        assertTrue(Files.exists(project.getEadToCdmExportPath()), "EAD to CDM export file not created");
        var format = CSVFormat.TDF.builder()
                .setTrim(true)
                .setSkipHeaderRecord(true)
                .setHeader(TSV_STANDARDIZED_HEADERS)
                .get();

        try (
                Reader reader = Files.newBufferedReader(project.getEadToCdmExportPath());
                var tsvParser = CSVParser.parse(reader, format);
        ) {
            var tsvRecord = tsvParser.getRecords().getFirst();
            assertEquals("Joyner Family Papers, ; 4428", tsvRecord.get(STANDARDIZED_COLLECTION_NAME));
            assertEquals("Folder", tsvRecord.get(STANDARDIZED_CONTAINER_TYPE));
            assertEquals("", tsvRecord.get(STANDARDIZED_GEOGRAPHIC_NAME));
        }
    }

    @Test
    public void exportEadToCdmApiCollectionNotFoundTest() throws Exception {
        eadToCdmApiResponse("nope", "[\"nope was not found\"]");
        Path projPath = createProject("nope_ead");
        String[] args = exportArgs(projPath, "-ead");
        executeExpectFailure(args);
    }

    private void assertDescAllFilePresent(MigrationProject project, String expectedContentPath) throws Exception {
        assertEquals(IOUtils.toString(getClass().getResourceAsStream(expectedContentPath), StandardCharsets.UTF_8),
                FileUtils.readFileToString(CdmFileRetrievalService.getDescAllPath(project).toFile(), StandardCharsets.UTF_8));
    }

    private void assertCpdFilePresent(MigrationProject project, String exportedFilename, String expectedContentPath) throws Exception {
        assertEquals(IOUtils.toString(getClass().getResourceAsStream(expectedContentPath), StandardCharsets.UTF_8),
                FileUtils.readFileToString(CdmFileRetrievalService.getExportedCpdsPath(project).resolve(exportedFilename).toFile(), StandardCharsets.UTF_8));
    }

    private Path createProject() throws Exception {
        return createProject(COLLECTION_ID);
    }

    private Path createProject(String collId) throws Exception {
        MigrationProject project = MigrationProjectFactory.createCdmMigrationProject(
                baseDir, collId, null, USERNAME,
                CdmEnvironmentHelper.DEFAULT_ENV_ID, BxcEnvironmentHelper.DEFAULT_ENV_ID);
        CdmFieldInfo fieldInfo = new CdmFieldInfo();
        CdmFieldEntry fieldEntry = new CdmFieldEntry();
        fieldEntry.setNickName("title");
        fieldEntry.setExportAs("titla");
        fieldEntry.setDescription("Title");
        fieldInfo.getFields().add(fieldEntry);
        CdmFieldEntry fieldEntry2 = new CdmFieldEntry();
        fieldEntry2.setNickName("title2");
        fieldEntry2.setExportAs("titlb");
        fieldEntry2.setDescription("Another Title");
        fieldInfo.getFields().add(fieldEntry2);
        fieldService.persistFieldsToProject(project, fieldInfo);
        return project.getProjectPath();
    }

    private void eadToCdmApiResponse(String eadId, String apiResponse) {
        stubFor(get(urlEqualTo("/ead/" + eadId))
                .willReturn(aResponse()
                        .withBody(apiResponse)
                        .withHeader("Content-Type", "text/json")));
    }
}
