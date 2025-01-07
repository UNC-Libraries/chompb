package edu.unc.lib.boxc.migration.cdm;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

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

    private void indexExportSamples() throws Exception {
        testHelper.indexExportData("mini_gilmer");
    }
}
