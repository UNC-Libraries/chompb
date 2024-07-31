package edu.unc.lib.boxc.migration.cdm;

import edu.unc.lib.boxc.migration.cdm.model.ExportObjectsInfo;
import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo;
import edu.unc.lib.boxc.migration.cdm.services.ExportObjectsService;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.test.BxcEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ExportObjectsCommandIT extends AbstractCommandIT {
    private static final String PROJECT_NAME = "proj";
    private ExportObjectsService exportObjectsService;

    @BeforeEach
    public void setup() throws Exception {
        project = MigrationProjectFactory.createCdmMigrationProject(
                tmpFolder, PROJECT_NAME, null, "user",
                null, BxcEnvironmentHelper.DEFAULT_ENV_ID);
        exportObjectsService = new ExportObjectsService();
    }

    @Test
    public void exportObjectsNoSourceFileTest() throws Exception {
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "export_objects"
        };

        executeExpectFailure(args);
        assertOutputContains("Failed to export objects in proj: InvalidProjectStateException: " +
                "Source files must be mapped");
    }

    @Test
    public void exportObjectsTest() throws Exception {
        writeSourceCsv(sourceMappingBody("testid,," + filesystemSourceFile("IMG_2377.jpeg") + ",",
                "test-00001,," + filesystemSourceFile("D2_035_Varners_DrugStore_interior.tif") + ",",
                "test-00002,," + filesystemSourceFile("MJM_7_016_LumberMills_IndianCreekTrestle.tif") + ","));
        project.getProjectProperties().setSourceFilesUpdatedDate(Instant.now());
        ProjectPropertiesSerialization.write(project);

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "export_objects"
        };
        executeExpectSuccess(args);

        Path exportedObjectsPath = project.getExportObjectsPath();
        assertTrue(Files.exists(exportedObjectsPath));
        List<CSVRecord> rows = listCsvRecords(exportedObjectsPath);
        assertEquals(3, rows.size());
        assertIterableEquals(Arrays.asList("testid", "src/test/resources/files/IMG_2377.jpeg",
                "IMG_2377.jpeg"), rows.get(0));
        assertIterableEquals(Arrays.asList("test-00001",
                "src/test/resources/files/D2_035_Varners_DrugStore_interior.tif",
                "D2_035_Varners_DrugStore_interior.tif"), rows.get(1));
        assertIterableEquals(Arrays.asList("test-00002",
                "src/test/resources/files/MJM_7_016_LumberMills_IndianCreekTrestle.tif",
                "MJM_7_016_LumberMills_IndianCreekTrestle.tif"), rows.get(2));
    }

    private String sourceMappingBody(String... rows) {
        return String.join(",", SourceFilesInfo.CSV_HEADERS) + "\n"
                + String.join("\n", rows);
    }

    private void writeSourceCsv(String mappingBody) throws IOException {
        FileUtils.write(project.getSourceFilesMappingPath().toFile(),
                mappingBody, StandardCharsets.UTF_8);
    }

    private Path filesystemSourceFile(String relPath) {
        Path basePath = Path.of("src/test/resources/files");
        return basePath.resolve(relPath);
    }

    private List<CSVRecord> listCsvRecords(Path exportedObjectsPath) throws Exception {
        List<CSVRecord> rows;
        try (
                Reader reader = Files.newBufferedReader(exportedObjectsPath);
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                        .withFirstRecordAsHeader()
                        .withHeader(ExportObjectsInfo.CSV_HEADERS)
                        .withTrim());
        ) {
            rows = csvParser.getRecords();
        }
        return rows;
    }
}
