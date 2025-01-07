package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.AltTextOptions;
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

import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.MockitoAnnotations.openMocks;

public class AltTextServiceTest {
    private static final String PROJECT_NAME = "proj";

    @TempDir
    public Path tmpFolder;
    private MigrationProject project;
    private SipServiceHelper testHelper;
    private AltTextService service;

    private AutoCloseable closeable;

    @BeforeEach
    public void setup() throws Exception {
        closeable = openMocks(this);
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder, PROJECT_NAME, null, "user", CdmEnvironmentHelper.DEFAULT_ENV_ID,
                BxcEnvironmentHelper.DEFAULT_ENV_ID, MigrationProject.PROJECT_SOURCE_CDM);
        Files.createDirectories(project.getExportPath());

        testHelper = new SipServiceHelper(project, tmpFolder);
        service = testHelper.getAltTextService();
        service.setProject(project);
        service.setIndexService(testHelper.getIndexService());
    }

    @AfterEach
    void closeService() throws Exception {
        closeable.close();
    }

    @Test
    public void generateTemplateTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        service.generateTemplate();

        assertTrue(Files.exists(project.getAltTextCsvPath()));

        try (
                Reader reader = Files.newBufferedReader(project.getAltTextCsvPath());
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                        .withFirstRecordAsHeader()
                        .withHeader(AltTextService.ALT_TEXT_CSV_HEADERS)
                        .withTrim());
        ) {
            List<CSVRecord> rows = csvParser.getRecords();
            assertEquals("25", rows.get(0).get(CdmFieldInfo.CDM_ID));
            assertEquals("26", rows.get(1).get(CdmFieldInfo.CDM_ID));
            assertEquals("27", rows.get(2).get(CdmFieldInfo.CDM_ID));
        }
    }

    @Test
    public void uploadCsvTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        var options = new AltTextOptions();
        options.setAltTextCsvFile(Path.of("src/test/resources/alt_text/mini_gilmer_alttext.csv"));

        service.uploadCsv(options);
        assertTrue(Files.exists(project.getAltTextPath()));
        assertTrue(Files.exists(project.getAltTextPath().resolve("25.txt")));
        // cdmrecord 26 has no alt-text
        assertFalse(Files.exists(project.getAltTextPath().resolve("26.txt")));
        assertTrue(Files.exists(project.getAltTextPath().resolve("27.txt")));
    }
}
