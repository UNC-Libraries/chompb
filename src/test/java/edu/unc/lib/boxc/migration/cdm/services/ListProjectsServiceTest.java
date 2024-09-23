package edu.unc.lib.boxc.migration.cdm.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo;
import edu.unc.lib.boxc.migration.cdm.test.BxcEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.SipServiceHelper;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.MockitoAnnotations.openMocks;

public class ListProjectsServiceTest {
    private static final String PROJECT_NAME = "proj";
    private static final String PROJECT_NAME_2 = "proj2";

    @TempDir
    public Path tmpFolder;

    private SipServiceHelper testHelper;
    private MigrationProject project;
    private ListProjectsService service;
    private CdmFieldService fieldService;
    private CdmIndexService indexService;
    private ProjectPropertiesService projectPropertiesService;
    private SourceFileService sourceFileService;
    private ArchiveProjectService archiveProjectService;

    private AutoCloseable closeable;

    @BeforeEach
    public void setup() throws Exception {
        closeable = openMocks(this);
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder, PROJECT_NAME, null, "user", CdmEnvironmentHelper.DEFAULT_ENV_ID,
                BxcEnvironmentHelper.DEFAULT_ENV_ID, MigrationProject.PROJECT_SOURCE_CDM);
        testHelper = new SipServiceHelper(project, tmpFolder);

        fieldService = new CdmFieldService();
        indexService = new CdmIndexService();
        indexService.setFieldService(fieldService);
        sourceFileService = new SourceFileService();
        sourceFileService.setIndexService(indexService);
        projectPropertiesService = new ProjectPropertiesService();
        archiveProjectService = new ArchiveProjectService();
        service = new ListProjectsService();
        service.setFieldService(fieldService);
        service.setIndexService(indexService);
        service.setPropertiesService(projectPropertiesService);
        service.setSourceFileService(sourceFileService);
    }

    @AfterEach
    void closeService() throws Exception {
        closeable.close();
    }

    @Test
    public void invalidDirectoryTest() throws Exception {
        try {
            service.listProjects(Path.of("test"));
            fail();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Path test does not exist"));
        }
    }

    @Test
    public void allowedActionTest() throws Exception {
        writeSourceFilesCsv(mappingBody("test,," + Path.of("test.tif") + ","));
        JsonNode list = service.listProjects(tmpFolder);

        assertEquals(Path.of(tmpFolder + "/" + PROJECT_NAME).toString(),
                list.findValue(ListProjectsService.PROJECT_PATH).asText());
        assertEquals("sources_mapped", list.findValue(ListProjectsService.STATUS).asText());
        assertEquals(jsonArray(Arrays.asList("crop_color_bars")), list.findValue(ListProjectsService.ALLOWED_ACTIONS));
        assertEquals(PROJECT_NAME, list.findValue("name").asText());
    }

    @Test
    public void listProjectsInitializedTest() throws Exception {
        JsonNode list = service.listProjects(tmpFolder);

        assertEquals(Path.of(tmpFolder + "/" + PROJECT_NAME).toString(),
                list.findValue(ListProjectsService.PROJECT_PATH).asText());
        assertEquals("initialized", list.findValue(ListProjectsService.STATUS).asText());
        assertEquals(jsonArray(Arrays.asList()), list.findValue(ListProjectsService.ALLOWED_ACTIONS));
        assertEquals(PROJECT_NAME, list.findValue("name").asText());
    }

    @Test
    public void listProjectsIndexedTest() throws Exception {
        project.getProjectProperties().setIndexedDate(Instant.now());
        ProjectPropertiesSerialization.write(project);
        JsonNode list = service.listProjects(tmpFolder);

        assertEquals(Path.of(tmpFolder + "/" + PROJECT_NAME).toString(),
                list.findValue(ListProjectsService.PROJECT_PATH).asText());
        assertEquals("indexed", list.findValue(ListProjectsService.STATUS).asText());
        assertEquals(jsonArray(Arrays.asList()), list.findValue(ListProjectsService.ALLOWED_ACTIONS));
        assertEquals(PROJECT_NAME, list.findValue("name").asText());
    }

    @Test
    public void listProjectsSourcesMappedTest() throws Exception {
        project.getProjectProperties().setSourceFilesUpdatedDate(Instant.now());
        ProjectPropertiesSerialization.write(project);
        JsonNode list = service.listProjects(tmpFolder);

        assertEquals(Path.of(tmpFolder + "/" + PROJECT_NAME).toString(),
                list.findValue(ListProjectsService.PROJECT_PATH).asText());
        assertEquals("sources_mapped", list.findValue(ListProjectsService.STATUS).asText());
        assertEquals(jsonArray(Arrays.asList()), list.findValue(ListProjectsService.ALLOWED_ACTIONS));
        assertEquals(PROJECT_NAME, list.findValue("name").asText());
    }

    @Test
    public void listProjectsSipsGeneratedTest() throws Exception {
        project.getProjectProperties().setSourceFilesUpdatedDate(Instant.now());
        project.getProjectProperties().setSipsGeneratedDate(Instant.now());
        ProjectPropertiesSerialization.write(project);
        JsonNode list = service.listProjects(tmpFolder);

        assertEquals(Path.of(tmpFolder + "/" + PROJECT_NAME).toString(),
                list.findValue(ListProjectsService.PROJECT_PATH).asText());
        assertEquals("sips_generated", list.findValue(ListProjectsService.STATUS).asText());
        assertEquals(jsonArray(Arrays.asList()), list.findValue(ListProjectsService.ALLOWED_ACTIONS));
        assertEquals(PROJECT_NAME, list.findValue("name").asText());
    }

    @Test
    public void listProjectsSipsSubmittedTest() throws Exception {
        project.getProjectProperties().setSipsGeneratedDate(Instant.now());
        project.getProjectProperties().setSipsSubmitted(Collections.singleton("test"));
        ProjectPropertiesSerialization.write(project);
        JsonNode list = service.listProjects(tmpFolder);

        assertEquals(Path.of(tmpFolder + "/" + PROJECT_NAME).toString(),
                list.findValue(ListProjectsService.PROJECT_PATH).asText());
        assertEquals("ingested", list.findValue(ListProjectsService.STATUS).asText());
        assertEquals(jsonArray(Arrays.asList()), list.findValue(ListProjectsService.ALLOWED_ACTIONS));
        assertEquals(PROJECT_NAME, list.findValue("name").asText());
    }

    @Test
    public void listProjectsArchivedTest() throws Exception {
        List<Path> testProjects = new ArrayList<>();
        testProjects.add(tmpFolder.resolve(PROJECT_NAME));
        archiveProjectService.archiveProject(tmpFolder, testProjects);
        JsonNode list = service.listProjects(tmpFolder.resolve("archived"));

        assertEquals(Path.of(tmpFolder + "/archived/" + PROJECT_NAME).toString(),
                list.findValue(ListProjectsService.PROJECT_PATH).asText());
        assertEquals("archived", list.findValue(ListProjectsService.STATUS).asText());
        assertEquals(jsonArray(Arrays.asList()), list.findValue(ListProjectsService.ALLOWED_ACTIONS));
        assertEquals(PROJECT_NAME, list.findValue("name").asText());
    }

    @Test
    public void listProjectsMultipleProjectsTest() throws Exception {
        // project one ingested
        project.getProjectProperties().setSipsGeneratedDate(Instant.now());
        project.getProjectProperties().setSipsSubmitted(Collections.singleton("test"));
        ProjectPropertiesSerialization.write(project);

        // project two initialized
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder, PROJECT_NAME_2, null, "user", CdmEnvironmentHelper.DEFAULT_ENV_ID,
                BxcEnvironmentHelper.DEFAULT_ENV_ID, MigrationProject.PROJECT_SOURCE_CDM);

        JsonNode list = service.listProjects(tmpFolder);

        assertTrue(list.findValues(ListProjectsService.PROJECT_PATH).toString().contains(tmpFolder + "/" + PROJECT_NAME_2));
        assertTrue(list.findValues(ListProjectsService.PROJECT_PATH).toString().contains(tmpFolder + "/" + PROJECT_NAME));
        assertTrue(list.findValues(ListProjectsService.STATUS).toString().contains("initialized"));
        assertTrue(list.findValues(ListProjectsService.STATUS).toString().contains("ingested"));
        assertEquals(jsonArray(Arrays.asList()), list.findValue(ListProjectsService.ALLOWED_ACTIONS));
        assertTrue(list.findValues("name").toString().contains(PROJECT_NAME_2));
        assertTrue(list.findValues("name").toString().contains(PROJECT_NAME));
    }

    private String mappingBody(String... rows) {
        return String.join(",", SourceFilesInfo.CSV_HEADERS) + "\n"
                + String.join("\n", rows);
    }

    private void writeSourceFilesCsv(String mappingBody) throws IOException {
        FileUtils.write(project.getSourceFilesMappingPath().toFile(),
                mappingBody, StandardCharsets.UTF_8);
        project.getProjectProperties().setSourceFilesUpdatedDate(Instant.now());
        ProjectPropertiesSerialization.write(project);
    }

    private ArrayNode jsonArray(List<String> values) {
        ObjectMapper mapper = new ObjectMapper();
        ArrayNode arrayNode = mapper.valueToTree(values);
        return arrayNode;
    }
}
