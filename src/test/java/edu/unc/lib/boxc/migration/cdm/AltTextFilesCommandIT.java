package edu.unc.lib.boxc.migration.cdm;

import edu.unc.lib.boxc.migration.cdm.model.MigrationProjectProperties;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AltTextFilesCommandIT extends AbstractCommandIT {
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
                "alt_text_files", "generate",
                "-b", basePath.toString()};
        executeExpectFailure(args);

        assertOutputContains("Project must be indexed");

        assertUpdatedDateNotPresent();
    }

    @Test
    public void generateNoBasePathTest() throws Exception {
        indexExportSamples();
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "alt_text_files", "generate"};
        executeExpectFailure(args);

        assertOutputContains("Must provide a base path");

        assertUpdatedDateNotPresent();
    }

    @Test
    public void generateAltTextMappingSucceedsTest() throws Exception {
        indexExportSamples();
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "alt_text_files", "generate",
                "-b", basePath.toString()};
        executeExpectSuccess(args);

        assertTrue(Files.exists(project.getAltTextFilesMappingPath()));
        assertOutputMatches(".*Previous Files Mapped: +0.*");
        assertOutputMatches(".*New Files Mapped: +0.*");
        assertOutputMatches(".*Total Files Mapped: +0.*");
        assertOutputMatches(".*Total Files in Project: +3.*");

        assertUpdatedDatePresent();
    }

    @Test
    public void generateBasicMatchDryRunTest() throws Exception {
        indexExportSamples();

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "alt_text_files", "generate",
                "--dry-run",
                "-b", "src/test/resources/alt_text/"};
        executeExpectSuccess(args);

        assertFalse(Files.exists(project.getAltTextFilesMappingPath()));
        assertOutputMatches(".*Previous Files Mapped: +0.*");
        assertOutputMatches(".*New Files Mapped: +2.*");
        assertOutputMatches(".*Total Files Mapped: +2.*");
        assertOutputMatches(".*Total Files in Project: +3.*");
        assertOutputContains("25,,src/test/resources/alt_text/25_alt_text.txt");
        assertOutputContains("26,,src/test/resources/alt_text/26_alttext.txt,");

        assertUpdatedDateNotPresent();
    }

    private void indexExportSamples() throws Exception {
        testHelper.indexExportData("mini_gilmer");
    }

    private void assertUpdatedDatePresent() throws Exception {
        MigrationProjectProperties props = ProjectPropertiesSerialization.read(project.getProjectPropertiesPath());
        assertNotNull(props.getAltTextFilesUpdatedDate(), "Updated timestamp must be set");
        assertNull(props.getSourceFilesUpdatedDate(), "Source mapping timestamp must not be set");
    }

    private void assertUpdatedDateNotPresent() throws Exception {
        MigrationProjectProperties props = ProjectPropertiesSerialization.read(project.getProjectPropertiesPath());
        assertNull(props.getAltTextFilesUpdatedDate(), "Updated timestamp must not be set");
        assertNull(props.getSourceFilesUpdatedDate(), "Source mapping timestamp must not be set");
    }
}
