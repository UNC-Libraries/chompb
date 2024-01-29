package edu.unc.lib.boxc.migration.cdm;

import edu.unc.lib.boxc.migration.cdm.model.MigrationProjectProperties;
import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author bbpennel
 */
public class AccessFilesCommandIT extends AbstractCommandIT {


    private Path basePath;

    @BeforeEach
    public void setup() throws Exception {
        initProjectAndHelper();
        basePath = tmpFolder;
    }

    @Test
    public void generateNotIndexedTest() throws Exception {
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "access_files", "generate",
                "-b", basePath.toString(),
                "-n", "file"};
        executeExpectFailure(args);

        assertOutputContains("Project must be indexed");

        assertUpdatedDateNotPresent();
    }

    @Test
    public void generateNoExportFieldTest() throws Exception {
        indexExportSamples();
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "access_files", "generate",
                "-b", basePath.toString(),
                "-n", ""};
        executeExpectFailure(args);

        assertOutputContains("Must provide an export field");

        assertUpdatedDateNotPresent();
    }

    @Test
    public void generateNoBasePathTest() throws Exception {
        indexExportSamples();
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "access_files", "generate",
                "-n", "file"};
        executeExpectFailure(args);

        assertOutputContains("Must provide a base path");

        assertUpdatedDateNotPresent();
    }

    @Test
    public void generateBasicMatchSucceedsTest() throws Exception {
        indexExportSamples();
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "access_files", "generate",
                "-b", basePath.toString(),
                "-n", "file"};
        executeExpectSuccess(args);

        assertTrue(Files.exists(project.getAccessFilesMappingPath()));
        assertFalse(Files.exists(project.getSourceFilesMappingPath()));
        assertOutputMatches(".*New Files Mapped: +0.*");
        assertOutputMatches(".*Total Files Mapped: +0.*");
        assertOutputMatches(".*Total Files in Project: +3.*");

        assertUpdatedDatePresent();
    }

    @Test
    public void generateBasicMatchDryRunTest() throws Exception {
        indexExportSamples();
        Path srcPath1 = addSourceFile("276_182_E.tif");
        Path mappingPath = project.getAccessFilesMappingPath();
        Path tempMappingPath = mappingPath.getParent().resolve("~" + mappingPath.getFileName().toString() + "_new");

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "access_files", "generate",
                "--dry-run",
                "-b", basePath.toString()};
        executeExpectSuccess(args);

        assertTrue(Files.exists(tempMappingPath));
        try (
                Reader reader = Files.newBufferedReader(tempMappingPath);
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                        .withFirstRecordAsHeader()
                        .withHeader(SourceFilesInfo.CSV_HEADERS)
                        .withTrim());
        ) {
            List<CSVRecord> rows = csvParser.getRecords();
            assertIterableEquals(Arrays.asList("25", "276_182_E.tif", srcPath1.toString(), ""), rows.get(0));
            assertIterableEquals(Arrays.asList("26", "276_183_E.tif", "", ""), rows.get(1));
            assertIterableEquals(Arrays.asList("27", "276_203_E.tif", "", ""), rows.get(2));
        }
        assertOutputMatches(".*New Files Mapped: +0.*");
        assertOutputMatches(".*Total Files Mapped: +0.*");
        assertOutputMatches(".*Total Files in Project: +3.*");

        assertUpdatedDateNotPresent();
    }

    @Test
    public void generateNestedPatternMatchDryRunTest() throws Exception {
        indexExportSamples();
        Path srcPath1 = addSourceFile("path/to/00276_op0182_0001_e.tif");
        Path srcPath3 = addSourceFile("00276_op0203_0001_e.tif");
        Path mappingPath = project.getAccessFilesMappingPath();
        Path tempMappingPath = mappingPath.getParent().resolve("~" + mappingPath.getFileName().toString() + "_new");

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "access_files", "generate",
                "--dry-run",
                "-b", basePath.toString(),
                "-p", "(\\d+)\\_(\\d+)_E.tif",
                "-t", "00$1_op0$2_0001_e.tif" };
        executeExpectSuccess(args);

        assertTrue(Files.exists(tempMappingPath));
        try (
                Reader reader = Files.newBufferedReader(tempMappingPath);
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                        .withFirstRecordAsHeader()
                        .withHeader(SourceFilesInfo.CSV_HEADERS)
                        .withTrim());
        ) {
            List<CSVRecord> rows = csvParser.getRecords();
            assertIterableEquals(Arrays.asList("25", "276_182_E.tif", srcPath1.toString(), ""), rows.get(0));
            assertIterableEquals(Arrays.asList("26", "276_183_E.tif", "", ""), rows.get(1));
            assertIterableEquals(Arrays.asList("27", "276_203_E.tif", srcPath3.toString(), ""), rows.get(2));
        }
        assertOutputMatches(".*New Files Mapped: +0.*");
        assertOutputMatches(".*Total Files Mapped: +0.*");
        assertOutputMatches(".*Total Files in Project: +3.*");

        assertUpdatedDateNotPresent();
    }

    private void indexExportSamples() throws Exception {
        testHelper.indexExportData("mini_gilmer");
    }

    private Path addSourceFile(String relPath) throws IOException {
        Path srcPath = basePath.resolve(relPath);
        // Create parent directories in case they don't exist
        Files.createDirectories(srcPath.getParent());
        Files.createFile(srcPath);
        return srcPath;
    }

    private void assertUpdatedDatePresent() throws Exception {
        MigrationProjectProperties props = ProjectPropertiesSerialization.read(project.getProjectPropertiesPath());
        assertNotNull(props.getAccessFilesUpdatedDate(), "Updated timestamp must be set");
        assertNull(props.getSourceFilesUpdatedDate(), "Source mapping timestamp must not be set");
    }

    private void assertUpdatedDateNotPresent() throws Exception {
        MigrationProjectProperties props = ProjectPropertiesSerialization.read(project.getProjectPropertiesPath());
        assertNull(props.getAccessFilesUpdatedDate(), "Updated timestamp must not be set");
        assertNull(props.getSourceFilesUpdatedDate(), "Source mapping timestamp must not be set");
    }
}
