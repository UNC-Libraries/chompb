package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.AbstractOutputTest;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.FindingAidReportOptions;
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
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.MockitoAnnotations.openMocks;

public class FindingAidReportServiceTest extends AbstractOutputTest {
    private static final String PROJECT_NAME = "proj";

    @TempDir
    public Path tmpFolder;
    private Path basePath;
    private MigrationProject project;
    private FindingAidReportService service;
    private SipServiceHelper testHelper;

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

        service = new FindingAidReportService();
        service.setProject(project);
        service.setIndexService(testHelper.getIndexService());
        service.setFieldService(testHelper.getFieldService());
    }

    @AfterEach
    void closeService() throws Exception {
        closeable.close();
    }

    @Test
    public void descriFieldReportTest() throws Exception {
        testHelper.indexExportData("03883");
        FindingAidReportOptions options = new FindingAidReportOptions();
        options.setField("descri");

        service.fieldCountUniqueValuesReport(options);

        assertTrue(Files.exists(service.getFieldValuesReportPath(options)));
        try (CSVParser csvParser = parser(service.getFieldValuesReportPath(options))) {
            List<CSVRecord> rows = csvParser.getRecords();
            assertIterableEquals(Arrays.asList("descri_value", "count"), rows.get(0));
            assertIterableEquals(Arrays.asList("03883", "3"), rows.get(1));
            assertEquals(2, rows.size());
        }
    }

    @Test
    public void contriFieldReportTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        FindingAidReportOptions options = new FindingAidReportOptions();
        options.setField("contri");

        service.fieldCountUniqueValuesReport(options);

        assertTrue(Files.exists(service.getFieldValuesReportPath(options)));
        try (CSVParser csvParser = parser(service.getFieldValuesReportPath(options))) {
            List<CSVRecord> rows = csvParser.getRecords();
            assertIterableEquals(Arrays.asList("contri_value", "count"), rows.get(0));
            assertIterableEquals(Arrays.asList("Fitz, Newton.", "2"), rows.get(1));
            assertIterableEquals(Arrays.asList("(blank)", "1"), rows.get(2));
            assertEquals(3, rows.size());
        }
    }

    @Test
    public void hookIdReportTest() throws Exception {
        testHelper.indexExportData("03883");

        service.hookIdReport();

        assertTrue(Files.exists(service.getHookIdReportPath()));
        try (CSVParser csvParser = parser(service.getHookIdReportPath())) {
            List<CSVRecord> rows = csvParser.getRecords();
            assertIterableEquals(Arrays.asList("hookid", "count"), rows.get(0));
            assertIterableEquals(Arrays.asList("03883_folder_9", "2"), rows.get(1));
            assertIterableEquals(Arrays.asList("03883_folder_5", "1"), rows.get(2));
            assertEquals(3, rows.size());
        }
    }

    @Test
    public void assessCollectionReportWithFindingAidTest() throws Exception {
        testHelper.indexExportData(Paths.get("src/test/resources/roy_brown/cdm_fields.csv"), "03883");
        CdmFieldService fieldService = new CdmFieldService();
        FindingAidService findingAidService = new FindingAidService();
        findingAidService.setCdmFieldService(fieldService);
        findingAidService.setProject(project);
        findingAidService.recordFindingAid();

        service.collectionReport();

        assertOutputMatches(".*Hook id: +contri+.*");
        assertOutputMatches(".*Collection number: +descri+.*");
        assertOutputMatches(".*03883.*");
        assertOutputMatches(".*Records with collection ids: +3 \\(100.0%\\).*");
        assertOutputMatches(".*Records with hook ids: +3 \\(100.0%\\).*");
        assertOutputMatches(".*collec:.*3\\/3.*100%.*");
        assertOutputMatches(".*descri:.*3\\/3.*100%.*");
        assertOutputMatches(".*findin:.*3\\/3.*100%.*");
        assertOutputMatches(".*locati:.*3\\/3.*100%.*");
        assertOutputMatches(".*title:.*3\\/3.*100%.*");
        assertOutputMatches(".*prefer:.*3\\/3.*100%.*");
        assertOutputMatches(".*creato:.*3\\/3.*100%.*");
        assertOutputMatches(".*contri:.*3\\/3.*100%.*");
        assertOutputMatches(".*relatid:.*3\\/3.*100%.*");
    }

    @Test
    public void assessCollectionReportWithoutFindingAidTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");

        service.collectionReport();

        assertOutputMatches(".*Collection number is not set..*");
        assertOutputMatches(".*Possible collection numbers:.*");
        assertOutputMatches(".*collec:.*0\\/3.*0%.*");
        assertOutputMatches(".*descri:.*3\\/3.*100%.*");
        assertOutputMatches(".*findin:.*0\\/3.*0%.*");
        assertOutputMatches(".*locati:.*0\\/3.*0%.*");
        assertOutputMatches(".*title:.*3\\/3.*100%.*");
        assertOutputMatches(".*prefer:.*0\\/3.*0%.*");
        assertOutputMatches(".*creato:.*3\\/3.*100%.*");
        assertOutputMatches(".*contri:.*2\\/3.*67%.*");
        assertOutputMatches(".*relatid:.*0\\/3.*0%.*");
    }

    private CSVParser parser(Path csvPath) throws IOException {
        Reader reader = Files.newBufferedReader(csvPath);
        CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                .withTrim());
        return csvParser;
    }
}
