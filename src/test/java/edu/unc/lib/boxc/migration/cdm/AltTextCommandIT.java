package edu.unc.lib.boxc.migration.cdm;

import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AltTextCommandIT extends AbstractCommandIT {
    private Path basePath;

    @BeforeEach
    public void setup() throws Exception {
        initProjectAndHelper();
        basePath = tmpFolder;
        setupChompbConfig();
    }

    @Test
    public void uploadCsv() throws Exception {
        indexExportSamples();
        String altTextCsv = "src/test/resources/alt_text/mini_gilmer_alttext.csv";

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "alt_text", "upload",
                "-fc", altTextCsv};
        executeExpectSuccess(args);

        assertTrue(Files.exists(project.getAltTextPath()));
        assertTrue(Files.exists(project.getAltTextPath().resolve("25.txt")));
        // cdmrecord 26 has no alt-text
        assertFalse(Files.exists(project.getAltTextPath().resolve("26.txt")));
        assertTrue(Files.exists(project.getAltTextPath().resolve("27.txt")));
    }

    @Test
    public void generateTemplate() throws Exception {
        indexExportSamples();
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "alt_text", "template"};
        executeExpectSuccess(args);

        assertTrue(Files.exists(project.getAltTextCsvPath()));
    }


    @Test
    public void validateValidTest() throws Exception {
        indexExportSamples();
        Path csvPath = writeCsv(mappingBody("25,alt-text", "26,more alt-text", "27,so much alt-text"));

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "alt_text", "validate",
                "-fc", String.valueOf(csvPath) };
        executeExpectSuccess(args);

        assertOutputContains("PASS: Alt-text upload file at path " + csvPath + " is valid");
    }

    @Test
    public void validateInvalidTest() throws Exception {
        indexExportSamples();
        Path csvPath = writeCsv(mappingBody(",alt-text"));

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "alt_text", "validate",
                "-fc", String.valueOf(csvPath) };
        executeExpectFailure(args);

        assertOutputContains("FAIL: Alt-text upload file at path " + csvPath + " is invalid");
        assertOutputContains("- Invalid blank id at line 2");
        assertEquals(2, output.split("    - ").length, "Must only be two errors: " + output);
    }

    @Test
    public void statusValidTest() throws Exception {
        indexExportSamples();
        Path csvPath = writeCsv(mappingBody("25,alt-text", "26,more alt-text", "27,so much alt-text"));

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "alt_text", "status",
                "-fc", String.valueOf(csvPath) };
        executeExpectSuccess(args);

        assertOutputMatches(".*Last Updated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Objects with Alt-text: +3 \\(100.0%\\).*");
        assertOutputMatches(".*Objects without Alt-text: +0.*");
        assertOutputMatches(".*Alt-text Upload File Valid: +Yes\n.*");
    }

    @Test
    public void statusUnmappedVerboseTest() throws Exception {
        indexExportSamples();
        Path csvPath = writeCsv(mappingBody("25,alt-text", "26,more alt-text"));

        String[] args2 = new String[] {
                "-w", project.getProjectPath().toString(),
                "alt_text", "status",
                "-fc", String.valueOf(csvPath),
                "-v" };
        executeExpectSuccess(args2);

        assertOutputMatches(".*Last Updated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Objects with Alt-text: +2 \\(66.7%\\).*");
        assertOutputMatches(".*Objects without Alt-text: +1.*");
        assertOutputMatches(".*Objects without Alt-text:.*\n + \\* 27.*");
        assertOutputMatches(".*Alt-text Upload File Valid: +Yes\n.*");
    }

    private void indexExportSamples() throws Exception {
        testHelper.indexExportData("mini_gilmer");
    }


    private String mappingBody(String... rows) {
        return String.join(",", SourceFilesInfo.CSV_HEADERS) + "\n"
                + String.join("\n", rows);
    }

    private Path writeCsv(String mappingBody) throws IOException {
        Path csvPath = project.getAltTextCsvPath();
        FileUtils.write(csvPath.toFile(),
                mappingBody, StandardCharsets.UTF_8);
        ProjectPropertiesSerialization.write(project);
        return csvPath;
    }
}
