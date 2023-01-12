package edu.unc.lib.boxc.migration.cdm;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.unc.lib.boxc.migration.cdm.services.ChompbConfigService;
import edu.unc.lib.boxc.migration.cdm.test.BxcEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.github.tomakehurst.wiremock.junit5.WireMockTest;

import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo.CdmFieldEntry;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProjectProperties;
import edu.unc.lib.boxc.migration.cdm.services.CdmFieldService;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;

/**
 * @author bbpennel
 */
@WireMockTest(httpPort = CdmEnvironmentHelper.TEST_HTTP_PORT)
public class InitializeProjectCommandIT extends AbstractCommandIT {
    private final static String COLLECTION_ID = "my_coll";

    private CdmFieldService fieldService;

    @BeforeEach
    public void setUp() throws Exception {
        fieldService = new CdmFieldService();

        String validRespBody = IOUtils.toString(this.getClass().getResourceAsStream("/cdm_fields_resp.json"),
                StandardCharsets.UTF_8);

        stubFor(get(urlMatching("/.*")).atPriority(5)
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "text/html")
                        .withBody("Error looking up collection /collid<br>")));

        stubFor(get(urlEqualTo("/" + CdmFieldService.getFieldInfoUrl(COLLECTION_ID)))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "text/json")
                        .withBody(validRespBody)));

        setupChompbConfig();
    }

    @Test
    public void initValidProjectTest() throws Exception {
        String[] initArgs = new String[] {
                "-w", baseDir.toString(),
                "--env-config", chompbConfigPath,
                "init",
                "-p", COLLECTION_ID };
        executeExpectSuccess(initArgs);

        MigrationProject project = MigrationProjectFactory.loadMigrationProject(baseDir.resolve(COLLECTION_ID));
        MigrationProjectProperties properties = project.getProjectProperties();
        assertPropertiesSet(properties, COLLECTION_ID, COLLECTION_ID);

        assertTrue(Files.exists(project.getDescriptionsPath()), "Description folder not created");

        assertCdmFieldsPresent(project);
    }

    @Test
    public void initValidProjectUsingCurrentDirTest() throws Exception {
        Path projDir = baseDir.resolve("aproject");
        Files.createDirectory(projDir);
        String[] initArgs = new String[] {
                "-w", projDir.toString(),
                "--env-config", chompbConfigPath,
                "init",
                "-c", COLLECTION_ID };
        executeExpectSuccess(initArgs);

        MigrationProject project = MigrationProjectFactory.loadMigrationProject(projDir);
        MigrationProjectProperties properties = project.getProjectProperties();
        assertPropertiesSet(properties, "aproject", COLLECTION_ID);

        assertTrue(Files.exists(project.getDescriptionsPath()), "Description folder not created");

        assertCdmFieldsPresent(project);
    }

    @Test
    public void initCdmCollectioNotFoundTest() throws Exception {
        String[] initArgs = new String[] {
                "-w", baseDir.toString(),
                "--env-config", chompbConfigPath,
                "init",
                "-p", "unknowncoll" };
        executeExpectFailure(initArgs);

        assertOutputContains("No collection with ID 'unknowncoll' found on server");

        assertProjectDirectoryNotCreate();
    }

    @Test
    public void initCdmProjectAlreadyExistsTest() throws Exception {
        Path projDir = baseDir.resolve("aproject");
        Files.createDirectory(projDir);
        String[] initArgs = new String[] {
                "-w", projDir.toString(),
                "--env-config", chompbConfigPath,
                "init",
                "-c", COLLECTION_ID };
        executeExpectSuccess(initArgs);

        // Run it a second time, should cause a failure
        executeExpectFailure(initArgs);
        assertOutputContains("the directory already contains a migration project");

        // The migration project should still be there
        MigrationProject project = MigrationProjectFactory.loadMigrationProject(projDir);
        MigrationProjectProperties properties = project.getProjectProperties();
        assertPropertiesSet(properties, "aproject", COLLECTION_ID);

        assertTrue(Files.exists(project.getDescriptionsPath()), "Description folder not created");

        assertCdmFieldsPresent(project);
    }

    @Test
    public void initUnknownEnvTest() throws Exception {
        String[] initArgs = new String[] {
                "-w", baseDir.toString(),
                "--env-config", chompbConfigPath,
                "init",
                "-e", "what",
                "-p", COLLECTION_ID };
        executeExpectFailure(initArgs);

        assertOutputContains("Unknown cdm-env value");

        assertProjectDirectoryNotCreate();
    }

    @Test
    public void initUnknownBxcEnvTest() throws Exception {
        String[] initArgs = new String[] {
                "-w", baseDir.toString(),
                "--env-config", chompbConfigPath,
                "init",
                "-E", "what",
                "-p", COLLECTION_ID };
        executeExpectFailure(initArgs);

        assertOutputContains("Unknown bxc-env value");

        assertProjectDirectoryNotCreate();
    }

    @Test
    public void initNoEnvMappingPathTest() throws Exception {
        String[] initArgs = new String[] {
                "-w", baseDir.toString(),
                "init",
                "-p", COLLECTION_ID };
        executeExpectFailure(initArgs);

        assertOutputContains("Must provide an env-config option");

        assertProjectDirectoryNotCreate();
    }

    private void assertProjectDirectoryNotCreate() throws IOException {
        try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(baseDir)) {
            for (Path path : dirStream) {
                assertFalse(path.toFile().isDirectory(),
                        "Project directory should not have been created, so base dir should not contain directories");
            }
        }
    }

    private void assertPropertiesSet(MigrationProjectProperties properties, String expName, String expCollId) {
        assertEquals(USERNAME, properties.getCreator());
        assertEquals(expName, properties.getName(), "Project name did not match expected value");
        assertEquals(expCollId, properties.getCdmCollectionId(), "CDM Collection ID did not match expected value");
        assertNotNull(properties.getCreatedDate(), "Created date not set");
        assertNull(properties.getHookId());
        assertNull(properties.getCollectionNumber());
        assertEquals(CdmEnvironmentHelper.DEFAULT_ENV_ID, properties.getCdmEnvironmentId());
        assertEquals(BxcEnvironmentHelper.DEFAULT_ENV_ID, properties.getBxcEnvironmentId());
    }

    private void assertCdmFieldsPresent(MigrationProject project) throws IOException {
        CdmFieldInfo fieldInfo = fieldService.loadFieldsFromProject(project);
        List<CdmFieldEntry> fields = fieldInfo.getFields();
        assertHasFieldWithValue("title", "title", "Title", false, fields);
        assertHasFieldWithValue("creato", "creato", "Creator", false, fields);
        assertHasFieldWithValue("date", "date", "Creation Date", false, fields);
        assertHasFieldWithValue("data", "data", "Date", false, fields);
        assertHasFieldWithValue("digitb", "digitb", "Digital Collection", false, fields);
    }

    private void assertHasFieldWithValue(String nick, String expectedExport, String expectedDesc,
            boolean expectedSkip, List<CdmFieldEntry> fields) {
        Optional<CdmFieldEntry> matchOpt = fields.stream().filter(f -> nick.equals(f.getNickName())).findFirst();
        assertTrue(matchOpt.isPresent(), "Field " + nick + " not present");
        CdmFieldEntry entry = matchOpt.get();
        assertEquals(expectedDesc, entry.getDescription());
        assertEquals(expectedExport, entry.getExportAs());
        assertEquals(expectedSkip, entry.getSkipExport());
    }
}
