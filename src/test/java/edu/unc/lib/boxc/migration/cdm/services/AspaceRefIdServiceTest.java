package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.model.AspaceRefIdInfo;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProjectProperties;
import edu.unc.lib.boxc.migration.cdm.options.GroupMappingOptions;
import edu.unc.lib.boxc.migration.cdm.options.GroupMappingSyncOptions;
import edu.unc.lib.boxc.migration.cdm.test.BxcEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.SipServiceHelper;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.MockitoAnnotations.openMocks;

public class AspaceRefIdServiceTest {
    private static final String PROJECT_NAME = "proj";

    @TempDir
    public Path tmpFolder;
    private Path basePath;
    private MigrationProject project;
    private SipServiceHelper testHelper;
    private CdmIndexService indexService;
    private AspaceRefIdService service;

    private AutoCloseable closeable;

    @BeforeEach
    public void setup() throws Exception {
        closeable = openMocks(this);
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder, PROJECT_NAME, null, "user", CdmEnvironmentHelper.DEFAULT_ENV_ID,
                BxcEnvironmentHelper.DEFAULT_ENV_ID, MigrationProject.PROJECT_SOURCE_CDM);
        Files.createDirectories(project.getExportPath());

        basePath = tmpFolder.resolve("testFolder");
        Files.createDirectory(basePath);
        testHelper = new SipServiceHelper(project, basePath);
        indexService = testHelper.getIndexService();
        service = testHelper.getAspaceRefIdService();
        service.setProject(project);
        service.setIndexService(testHelper.getIndexService());
    }

    @AfterEach
    void closeService() throws Exception {
        closeable.close();
    }

    @Test
    public void generateWorkObjectsTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        service.generateAspaceRefIdMapping();

        assertTrue(Files.exists(project.getAspaceRefIdMappingPath()));

        try (CSVParser csvParser = parser()) {
            List<CSVRecord> rows = csvParser.getRecords();
            assertEquals("25", rows.get(0).get(AspaceRefIdInfo.RECORD_ID_FIELD));
            assertEquals("26", rows.get(1).get(AspaceRefIdInfo.RECORD_ID_FIELD));
            assertEquals("27", rows.get(2).get(AspaceRefIdInfo.RECORD_ID_FIELD));
            assertEquals(3, rows.size());
        }
    }

    @Test
    public void generateGroupObjectsTest() throws Exception {
        testHelper.indexExportData("grouped_gilmer");
        setupGroupIndex();

        service.generateAspaceRefIdMapping();

        assertTrue(Files.exists(project.getAspaceRefIdMappingPath()));

        try (CSVParser csvParser = parser()) {
            List<CSVRecord> rows = csvParser.getRecords();
            assertEquals("27", rows.get(0).get(AspaceRefIdInfo.RECORD_ID_FIELD));
            assertEquals("28", rows.get(1).get(AspaceRefIdInfo.RECORD_ID_FIELD));
            assertEquals("29", rows.get(2).get(AspaceRefIdInfo.RECORD_ID_FIELD));
            assertEquals("grp:groupa:group1", rows.get(3).get(AspaceRefIdInfo.RECORD_ID_FIELD));
            assertEquals(4, rows.size());
        }
    }

    @Test
    public void generateCompoundObjectsTest() throws Exception {
        testHelper.indexExportData("mini_keepsakes");
        service.generateAspaceRefIdMapping();

        assertTrue(Files.exists(project.getAspaceRefIdMappingPath()));

        try (CSVParser csvParser = parser()) {
            List<CSVRecord> rows = csvParser.getRecords();
            assertEquals("216", rows.get(0).get(AspaceRefIdInfo.RECORD_ID_FIELD));
            assertEquals("604", rows.get(1).get(AspaceRefIdInfo.RECORD_ID_FIELD));
            assertEquals("607", rows.get(2).get(AspaceRefIdInfo.RECORD_ID_FIELD));
            assertEquals(3, rows.size());
        }
    }

    @Test
    public void generatePdfCompoundObjectsTest() throws Exception {
        testHelper.indexExportData("pdf");
        service.generateAspaceRefIdMapping();

        assertTrue(Files.exists(project.getAspaceRefIdMappingPath()));

        try (CSVParser csvParser = parser()) {
            List<CSVRecord> rows = csvParser.getRecords();
            assertEquals("17940", rows.get(0).get(AspaceRefIdInfo.RECORD_ID_FIELD));
            assertEquals(1, rows.size());
        }
    }

    @Test
    public void syncNotIndexedTest() throws Exception {
        var e = assertThrows(InvalidProjectStateException.class, () -> {
            service.syncMappings();
        });
        assertExceptionContains("Project must be indexed", e);
        assertMappedDateNotPresent();
        assertSyncedDateNotPresent();
    }

    @Test
    public void syncNotGeneratedTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        var e = assertThrows(InvalidProjectStateException.class, () -> {
            service.syncMappings();
        });
        assertExceptionContains("Project has not previously generated aspace ref id mappings", e);
        assertMappedDateNotPresent();
        assertSyncedDateNotPresent();
    }

    @Test
    public void syncSingleRunTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        writeCsv(mappingBody("25,2817ec3c77e5ea9846d5c070d58d402b", "26,3817ec3c77e5ea9846d5c070d58d402b",
                "27,4817ec3c77e5ea9846d5c070d58d402b"));

        service.syncMappings();

        Connection conn = null;
        try {
            conn = indexService.openDbConnection();
            assertWorkSynced(conn, "25", "2817ec3c77e5ea9846d5c070d58d402b");
            assertWorkSynced(conn, "26", "3817ec3c77e5ea9846d5c070d58d402b");
            assertWorkSynced(conn, "27", "4817ec3c77e5ea9846d5c070d58d402b");
            assertSyncedDatePresent();
        } finally {
            CdmIndexService.closeDbConnection(conn);
        }
    }

    @Test
    public void syncSecondRunWithCleanupTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        writeCsv(mappingBody("25,2817ec3c77e5ea9846d5c070d58d402b", "26,3817ec3c77e5ea9846d5c070d58d402b",
                "27,4817ec3c77e5ea9846d5c070d58d402b"));

        service.syncMappings();

        Connection conn = null;
        try {
            conn = indexService.openDbConnection();
            assertWorkSynced(conn, "25", "2817ec3c77e5ea9846d5c070d58d402b");
            assertWorkSynced(conn, "26", "3817ec3c77e5ea9846d5c070d58d402b");
            assertWorkSynced(conn, "27", "4817ec3c77e5ea9846d5c070d58d402b");

            assertSyncedDatePresent();
        } finally {
            CdmIndexService.closeDbConnection(conn);
        }

        writeCsv(mappingBody("25,2817ec3c77e5ea9846d5c070d58d402b", "26,3817ec3c77e5ea9846d5c070d58d402b"));

        service.syncMappings();

        try {
            conn = indexService.openDbConnection();
            assertWorkSynced(conn, "25", "2817ec3c77e5ea9846d5c070d58d402b");
            assertWorkSynced(conn, "26", "3817ec3c77e5ea9846d5c070d58d402b");
            assertSyncedDatePresent();
        } finally {
            CdmIndexService.closeDbConnection(conn);
        }
    }

    private CSVParser parser() throws IOException {
        Reader reader = Files.newBufferedReader(project.getAspaceRefIdMappingPath());
        CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                .withFirstRecordAsHeader()
                .withHeader(AspaceRefIdInfo.CSV_HEADERS)
                .withTrim());
        return csvParser;
    }

    private String mappingBody(String... rows) {
        return String.join(",", AspaceRefIdInfo.CSV_HEADERS) + "\n"
                + String.join("\n", rows);
    }

    private void writeCsv(String mappingBody) throws IOException {
        FileUtils.write(project.getAspaceRefIdMappingPath().toFile(),
                mappingBody, StandardCharsets.UTF_8);
        project.getProjectProperties().setAspaceRefIdMappingsUpdatedDate(Instant.now());
        ProjectPropertiesSerialization.write(project);
    }

    private GroupMappingSyncOptions makeDefaultSyncOptions() {
        var options = new GroupMappingSyncOptions();
        options.setSortField("file");
        return options;
    }

    private void setupGroupIndex() throws Exception {
        GroupMappingOptions groupOptions = new GroupMappingOptions();
        groupOptions.setGroupFields(Arrays.asList("groupa"));
        GroupMappingService groupService = testHelper.getGroupMappingService();
        groupService.generateMapping(groupOptions);
        groupService.syncMappings(makeDefaultSyncOptions());
    }

    private void assertWorkSynced(Connection conn, String expectedCdmId, String expectedAspaceRefId)
            throws Exception {
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select * from " + CdmIndexService.TB_NAME
                + " where " + CdmFieldInfo.CDM_ID + " = " + expectedCdmId);
        while (rs.next()) {
            String cdmId = rs.getString(CdmFieldInfo.CDM_ID);
            String refId = rs.getString(CdmIndexService.ASPACE_REF_ID);
            assertEquals(expectedCdmId, cdmId);
            assertEquals(expectedAspaceRefId, refId);
        }
    }

    private void assertExceptionContains(String expected, Exception e) {
        assertTrue(e.getMessage().contains(expected),
                "Expected message exception to contain '" + expected + "', but was: " + e.getMessage());
    }

    private void assertMappedDateNotPresent() throws Exception {
        MigrationProjectProperties props = ProjectPropertiesSerialization.read(project.getProjectPropertiesPath());
        assertNull(props.getAspaceRefIdMappingsUpdatedDate());
    }

    private void assertSyncedDatePresent() throws Exception {
        MigrationProjectProperties props = ProjectPropertiesSerialization.read(project.getProjectPropertiesPath());
        assertNotNull(props.getAspaceRefIdMappingsSyncedDate());
    }

    private void assertSyncedDateNotPresent() throws Exception {
        MigrationProjectProperties props = ProjectPropertiesSerialization.read(project.getProjectPropertiesPath());
        assertNull(props.getAspaceRefIdMappingsUpdatedDate());
    }
}
