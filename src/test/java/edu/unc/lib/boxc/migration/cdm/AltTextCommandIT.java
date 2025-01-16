package edu.unc.lib.boxc.migration.cdm;

import edu.unc.lib.boxc.migration.cdm.model.AltTextInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProjectProperties;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AltTextCommandIT extends AbstractCommandIT {
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
                "alt_text", "generate"};
        executeExpectFailure(args);

        assertOutputContains("Project must be indexed");

        assertUpdatedDateNotPresent();
    }

    @Test
    public void generateAltTextMappingSucceedsTest() throws Exception {
        indexExportSamples();
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "alt_text", "generate"};
        executeExpectSuccess(args);

        assertTrue(Files.exists(project.getAltTextMappingPath()));

        assertUpdatedDatePresent();
    }

    @Test
    public void validateValidTest() throws Exception {
        indexExportSamples();
        writeCsv(mappingBody("25,alt text"));

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "alt_text", "validate" };
        executeExpectSuccess(args);

        assertOutputContains("PASS: Alt-text mapping at path " + project.getAltTextMappingPath() + " is valid");
    }

    @Test
    public void validateInvalidTest() throws Exception {
        indexExportSamples();
        writeCsv(mappingBody("25,"));

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "alt_text", "validate" };
        executeExpectFailure(args);

        assertOutputContains("FAIL: Alt-text mapping at path " + project.getAltTextMappingPath()
                + " is invalid");
        assertOutputContains("- No alt-text mapped at line 2");
        assertEquals(2, output.split("    - ").length, "Must only be two errors: " + output);
    }

    @Test
    public void statusValidTest() throws Exception {
        indexExportSamples();
        writeCsv(mappingBody("25,alt text",
                "26,more alt text",
                "27,so much alt text"));

        String[] args2 = new String[] {
                "-w", project.getProjectPath().toString(),
                "alt_text", "status" };
        executeExpectSuccess(args2);

        assertOutputMatches(".*Last Updated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Objects Mapped: +3 \\(100.0%\\).*");
        assertOutputMatches(".*Unmapped Objects: +0.*");
        assertOutputMatches(".*Mappings Valid: +Yes\n.*");
    }

    @Test
    public void statusUnmappedVerboseTest() throws Exception {
        indexExportSamples();
        writeCsv(mappingBody("25,alt text",
                "27,more alt text"));

        String[] args2 = new String[] {
                "-w", project.getProjectPath().toString(),
                "alt_text", "status",
                "-v" };
        executeExpectSuccess(args2);

        assertOutputMatches(".*Last Updated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Objects Mapped: +2 \\(66.7%\\).*");
        assertOutputMatches(".*Unmapped Objects: +1.*");
        assertOutputMatches(".*Unmapped Objects:.*\n + \\* 26.*");
        assertOutputMatches(".*Mappings Valid: +Yes.*");
    }

    private void indexExportSamples() throws Exception {
        testHelper.indexExportData("mini_gilmer");
    }

    private void assertUpdatedDatePresent() throws Exception {
        MigrationProjectProperties props = ProjectPropertiesSerialization.read(project.getProjectPropertiesPath());
        assertNotNull(props.getAltTextFilesUpdatedDate(), "Updated timestamp must be set");
    }

    private void assertUpdatedDateNotPresent() throws Exception {
        MigrationProjectProperties props = ProjectPropertiesSerialization.read(project.getProjectPropertiesPath());
        assertNull(props.getAltTextFilesUpdatedDate(), "Updated timestamp must not be set");
    }

    private String mappingBody(String... rows) {
        return String.join(",", AltTextInfo.CSV_HEADERS) + "\n"
                + String.join("\n", rows);
    }

    private void writeCsv(String mappingBody) throws IOException {
        FileUtils.write(project.getAltTextMappingPath().toFile(),
                mappingBody, StandardCharsets.UTF_8);
        project.getProjectProperties().setAltTextFilesUpdatedDate(Instant.now());
        ProjectPropertiesSerialization.write(project);
    }
}
