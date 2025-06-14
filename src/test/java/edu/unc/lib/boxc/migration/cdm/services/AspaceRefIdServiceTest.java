package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.model.AspaceRefIdInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
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
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

    private CSVParser parser() throws IOException {
        Reader reader = Files.newBufferedReader(project.getAspaceRefIdMappingPath());
        CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                .withFirstRecordAsHeader()
                .withHeader(AspaceRefIdInfo.CSV_HEADERS)
                .withTrim());
        return csvParser;
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
}
