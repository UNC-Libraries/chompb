package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.model.AspaceRefIdInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.GroupMappingOptions;
import edu.unc.lib.boxc.migration.cdm.options.GroupMappingSyncOptions;
import edu.unc.lib.boxc.migration.cdm.test.BxcEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.SipServiceHelper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
        indexService = testHelper.getCdmIndexService();
        service = testHelper.getAspaceRefIdService();
        service.setProject(project);
        service.setFieldService(testHelper.getFieldService());
        service.setIndexService(testHelper.getCdmIndexService());
    }

    @AfterEach
    void closeService() throws Exception {
        closeable.close();
    }

    @Test
    public void generateWorkObjectsTest() throws Exception {
        testHelper.indexExportData(Paths.get("src/test/resources/findingaid_fields.csv"), "03883");
        service.generateBlankAspaceRefIdMapping();

        assertTrue(Files.exists(project.getAspaceRefIdMappingPath()));

        try (CSVParser csvParser = parser()) {
            List<CSVRecord> rows = csvParser.getRecords();
            assertEquals("0", rows.get(0).get(AspaceRefIdInfo.RECORD_ID_FIELD));
            assertEquals("548", rows.get(1).get(AspaceRefIdInfo.RECORD_ID_FIELD));
            assertEquals("549", rows.get(2).get(AspaceRefIdInfo.RECORD_ID_FIELD));
            assertEquals(3, rows.size());
        }
    }

    @Test
    public void generateGroupedObjectsTest() throws Exception {
        testHelper.indexExportData("grouped_gilmer");
        setupGroupIndex();

        service.generateBlankAspaceRefIdMapping();

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
    public void generatePdfCompoundObjectsTest() throws Exception {
        testHelper.indexExportData(Paths.get("src/test/resources/pdf_fields.csv"), "pdf");
        service.generateBlankAspaceRefIdMapping();

        assertTrue(Files.exists(project.getAspaceRefIdMappingPath()));

        try (CSVParser csvParser = parser()) {
            List<CSVRecord> rows = csvParser.getRecords();
            assertEquals("17940", rows.get(0).get(AspaceRefIdInfo.RECORD_ID_FIELD));
            assertEquals(1, rows.size());
        }
    }

    @Test
    public void generateFromHookIdRefIdCsvTest() throws Exception {
        testHelper.indexExportData(Paths.get("src/test/resources/findingaid_fields.csv"), "03883");
        service.setHookIdRefIdMapPath(Paths.get("src/test/resources/hookid_to_refid_map.csv"));
        service.generateAspaceRefIdMappingFromHookIdRefIdCsv();

        assertTrue(Files.exists(project.getAspaceRefIdMappingPath()));
        try (CSVParser csvParser = parser()) {
            List<CSVRecord> rows = csvParser.getRecords();
            assertEquals("0", rows.get(0).get(AspaceRefIdInfo.RECORD_ID_FIELD));
            assertEquals("03883_folder_5", rows.get(0).get(AspaceRefIdInfo.HOOK_ID_FIELD));
            assertEquals("8578708eda77e378b3a844a2166b815b", rows.get(0).get(AspaceRefIdInfo.REF_ID_FIELD));
            assertEquals("548", rows.get(1).get(AspaceRefIdInfo.RECORD_ID_FIELD));
            assertEquals("03883_folder_9", rows.get(1).get(AspaceRefIdInfo.HOOK_ID_FIELD));
            assertEquals("4c1196b46a06b21b1184fba0de1e84bd", rows.get(1).get(AspaceRefIdInfo.REF_ID_FIELD));
            assertEquals("549", rows.get(2).get(AspaceRefIdInfo.RECORD_ID_FIELD));
            assertEquals("03883_folder_9", rows.get(2).get(AspaceRefIdInfo.HOOK_ID_FIELD));
            assertEquals("4c1196b46a06b21b1184fba0de1e84bd", rows.get(2).get(AspaceRefIdInfo.REF_ID_FIELD));
            assertEquals(3, rows.size());
        }
    }

    @Test
    public void generateFromHookIdRefIdCsvNoRefIdTest() throws Exception {
        testHelper.indexExportData(Paths.get("src/test/resources/findingaid_fields.csv"), "03883");
        service.setHookIdRefIdMapPath(Paths.get("src/test/resources/hookid_to_refid_map2.csv"));
        service.generateAspaceRefIdMappingFromHookIdRefIdCsv();

        assertTrue(Files.exists(project.getAspaceRefIdMappingPath()));
        try (CSVParser csvParser = parser()) {
            List<CSVRecord> rows = csvParser.getRecords();
            assertEquals("0", rows.get(0).get(AspaceRefIdInfo.RECORD_ID_FIELD));
            assertEquals("03883_folder_5", rows.get(0).get(AspaceRefIdInfo.HOOK_ID_FIELD));
            assertEquals("", rows.get(0).get(AspaceRefIdInfo.REF_ID_FIELD));
            assertEquals("548", rows.get(1).get(AspaceRefIdInfo.RECORD_ID_FIELD));
            assertEquals("03883_folder_9", rows.get(1).get(AspaceRefIdInfo.HOOK_ID_FIELD));
            assertEquals("4c1196b46a06b21b1184fba0de1e84bd", rows.get(1).get(AspaceRefIdInfo.REF_ID_FIELD));
            assertEquals("549", rows.get(2).get(AspaceRefIdInfo.RECORD_ID_FIELD));
            assertEquals("03883_folder_9", rows.get(2).get(AspaceRefIdInfo.HOOK_ID_FIELD));
            assertEquals("4c1196b46a06b21b1184fba0de1e84bd", rows.get(2).get(AspaceRefIdInfo.REF_ID_FIELD));
            assertEquals(3, rows.size());
        }
    }

    @Test
    public void generateFromHookIdRefIdCsvWithZTest() throws Exception {
        // export collection with -z at the end of the collection ID
        testHelper.indexExportData(Paths.get("src/test/resources/findingaid_fields.csv"), "03883-z");
        service.setHookIdRefIdMapPath(Paths.get("src/test/resources/hookid_to_refid_map.csv"));
        service.generateAspaceRefIdMappingFromHookIdRefIdCsv();

        assertTrue(Files.exists(project.getAspaceRefIdMappingPath()));
        try (CSVParser csvParser = parser()) {
            List<CSVRecord> rows = csvParser.getRecords();
            assertEquals("0", rows.get(0).get(AspaceRefIdInfo.RECORD_ID_FIELD));
            assertEquals("03883_folder_5", rows.get(0).get(AspaceRefIdInfo.HOOK_ID_FIELD));
            assertEquals("8578708eda77e378b3a844a2166b815b", rows.get(0).get(AspaceRefIdInfo.REF_ID_FIELD));
            assertEquals("548", rows.get(1).get(AspaceRefIdInfo.RECORD_ID_FIELD));
            assertEquals("03883_folder_9", rows.get(1).get(AspaceRefIdInfo.HOOK_ID_FIELD));
            assertEquals("4c1196b46a06b21b1184fba0de1e84bd", rows.get(1).get(AspaceRefIdInfo.REF_ID_FIELD));
            assertEquals("549", rows.get(2).get(AspaceRefIdInfo.RECORD_ID_FIELD));
            assertEquals("03883_folder_9", rows.get(2).get(AspaceRefIdInfo.HOOK_ID_FIELD));
            assertEquals("4c1196b46a06b21b1184fba0de1e84bd", rows.get(2).get(AspaceRefIdInfo.REF_ID_FIELD));
            assertEquals(3, rows.size());
        }
    }

    @Test
    public void generateFromHookIdRefIdCsvNoContriDescriTest() throws Exception {
        testHelper.indexExportData(Paths.get("src/test/resources/monograph_fields.csv"), "monograph");

        Exception exception = assertThrows(InvalidProjectStateException.class, () -> {
            service.generateAspaceRefIdMappingFromHookIdRefIdCsv();
        });
        String expectedMessage = "Project has no contri field named hook id and/or descri field named collection number";
        String actualMessage = exception.getMessage();

        assertTrue(actualMessage.contains(expectedMessage));
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
