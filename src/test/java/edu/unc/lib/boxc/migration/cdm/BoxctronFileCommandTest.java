package edu.unc.lib.boxc.migration.cdm;

import edu.unc.lib.boxc.migration.cdm.model.MigrationProjectProperties;
import edu.unc.lib.boxc.migration.cdm.services.BoxctronFileService;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BoxctronFileCommandTest extends AbstractCommandIT {
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
                "boxctron_files", "generate"};
        executeExpectFailure(args);

        assertOutputContains("Project must be indexed");

        MigrationProjectProperties props = ProjectPropertiesSerialization.read(project.getProjectPropertiesPath());
        assertNull(props.getAccessFilesUpdatedDate(), "Updated timestamp must not be set");
        assertNull(props.getSourceFilesUpdatedDate(), "Source mapping timestamp must be set");
    }

    @Test
    public void generateBasicMatchSucceedsTest() throws Exception {
        indexExportSamples();
        testHelper.populateSourceFiles("276_182_E.tif", "276_183_E.tif", "276_203_E.tif");
        Path boxctronPath1 = tmpFolder.resolve("source/276_182_E.tif");
        boxctronWriteCsv(boxctronMappingBody(boxctronPath1 + ",1,0.9,\"[0.0, 0.9, 1.0, 1.0]\","));

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "boxctron_files", "generate"};
        executeExpectSuccess(args);

        assertTrue(Files.exists(project.getAccessFilesMappingPath()));
        assertOutputMatches(".*Previous Files Mapped: +0.*");
        assertOutputMatches(".*New Files Mapped: +1.*");
        assertOutputMatches(".*Total Files Mapped: +1.*");
        assertOutputMatches(".*Total Files in Project: +3.*");
        Path accessPath1 = project.getProjectPath().resolve("processing/results/velocicroptor/output"
                + boxctronPath1 + ".jpg");
        assertOutputContains("25,276_182_E.tif," + accessPath1);

        assertUpdatedDatePresent();
    }

    @Test
    public void generateBasicMatchDryRunTest() throws Exception {
        indexExportSamples();
        testHelper.populateSourceFiles("276_182_E.tif", "276_183_E.tif", "276_203_E.tif");
        Path boxctronPath1 = tmpFolder.resolve("source/276_182_E.tif");
        boxctronWriteCsv(boxctronMappingBody(boxctronPath1 + ",1,0.9,\"[0.0, 0.9, 1.0, 1.0]\","));

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "boxctron_files", "generate",
                "--dry-run"};
        executeExpectSuccess(args);

        assertFalse(Files.exists(project.getAccessFilesMappingPath()));
        assertOutputMatches(".*Previous Files Mapped: +0.*");
        assertOutputMatches(".*New Files Mapped: +1.*");
        assertOutputMatches(".*Total Files Mapped: +1.*");
        assertOutputMatches(".*Total Files in Project: +3.*");
        Path accessPath1 = project.getProjectPath().resolve("processing/results/velocicroptor/output"
                + boxctronPath1 + ".jpg");
        assertOutputContains("25,276_182_E.tif," + accessPath1);

        assertUpdatedDateNotPresent();
    }

    private void indexExportSamples() throws Exception {
        testHelper.indexExportData("mini_gilmer");
    }

    private String boxctronMappingBody(String... rows) {
        return String.join(",", BoxctronFileService.DATA_CSV_HEADERS) + "\n"
                + String.join("\n", rows);
    }

    private void boxctronWriteCsv(String boxctronMappingBody) throws IOException {
        FileUtils.write(project.getVelocicroptorDataPath().toFile(),
                boxctronMappingBody, StandardCharsets.UTF_8);
    }

    private void assertUpdatedDatePresent() throws Exception {
        MigrationProjectProperties props = ProjectPropertiesSerialization.read(project.getProjectPropertiesPath());
        assertNotNull(props.getAccessFilesUpdatedDate(), "Updated timestamp must be set");
        assertNotNull(props.getSourceFilesUpdatedDate(), "Source mapping timestamp must be set");
    }

    private void assertUpdatedDateNotPresent() throws Exception {
        MigrationProjectProperties props = ProjectPropertiesSerialization.read(project.getProjectPropertiesPath());
        assertNull(props.getAccessFilesUpdatedDate(), "Updated timestamp must not be set");
        assertNotNull(props.getSourceFilesUpdatedDate(), "Source mapping timestamp must be set");
    }
}
