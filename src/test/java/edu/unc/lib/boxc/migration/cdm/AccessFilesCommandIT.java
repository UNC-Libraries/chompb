package edu.unc.lib.boxc.migration.cdm;

import edu.unc.lib.boxc.migration.cdm.model.MigrationProjectProperties;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
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
        assertOutputMatches(".*Previous Files Mapped: +0.*");
        assertOutputMatches(".*New Files Mapped: +0.*");
        assertOutputMatches(".*Total Files Mapped: +0.*");
        assertOutputMatches(".*Total Files in Project: +3.*");

        assertUpdatedDatePresent();
    }

    @Test
    public void generateBasicMatchDryRunTest() throws Exception {
        indexExportSamples();
        Path srcPath1 = addSourceFile("276_182_E.tif");

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "access_files", "generate",
                "--dry-run",
                "-b", basePath.toString()};
        executeExpectSuccess(args);

        assertFalse(Files.exists(project.getAccessFilesMappingPath()));
        assertOutputMatches(".*Previous Files Mapped: +0.*");
        assertOutputMatches(".*New Files Mapped: +1.*");
        assertOutputMatches(".*Total Files Mapped: +1.*");
        assertOutputMatches(".*Total Files in Project: +3.*");
        assertOutputContains("25,276_182_E.tif," + srcPath1);

        assertUpdatedDateNotPresent();
    }

    @Test
    public void generateNestedPatternMatchDryRunTest() throws Exception {
        indexExportSamples();
        Path srcPath1 = addSourceFile("path/to/00276_op0182_0001_e.tif");
        Path srcPath2 = addSourceFile("00276_op0203_0001_e.tif");

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "access_files", "generate",
                "--dry-run",
                "-b", basePath.toString(),
                "-p", "(\\d+)\\_(\\d+)_E.tif",
                "-t", "00$1_op0$2_0001_e.tif" };
        executeExpectSuccess(args);

        assertFalse(Files.exists(project.getAccessFilesMappingPath()));
        assertOutputMatches(".*Previous Files Mapped: +0.*");
        assertOutputMatches(".*New Files Mapped: +2.*");
        assertOutputMatches(".*Total Files Mapped: +2.*");
        assertOutputMatches(".*Total Files in Project: +3.*");
        assertOutputContains("25,276_182_E.tif," + srcPath1);
        assertOutputContains("27,276_203_E.tif," + srcPath2);

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
