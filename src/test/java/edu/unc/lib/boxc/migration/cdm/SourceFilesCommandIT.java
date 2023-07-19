package edu.unc.lib.boxc.migration.cdm;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author bbpennel
 */
public class SourceFilesCommandIT extends AbstractCommandIT {
    private Path basePath;

    @BeforeEach
    public void setup() throws Exception {
        initProjectAndHelper();
        basePath = testHelper.getSourceFilesBasePath();
        Files.createDirectories(basePath);
    }

    @Test
    public void generateNotIndexedTest() throws Exception {
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "source_files", "generate",
                "-b", basePath.toString(),
                "-n", "file"};
        executeExpectFailure(args);

        assertOutputContains("Project must be indexed");
    }

    @Test
    public void generateNoExportFieldTest() throws Exception {
        indexExportSamples();
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "source_files", "generate",
                "-b", basePath.toString(),
                "-n", ""};
        executeExpectFailure(args);

        assertOutputContains("Must provide an export field");
    }

    @Test
    public void generateNoBasePathTest() throws Exception {
        indexExportSamples();
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "source_files", "generate",
                "-n", "file"};
        executeExpectFailure(args);

        assertOutputContains("Must provide a base path");
    }

    @Test
    public void generateBasicMatchSucceedsTest() throws Exception {
        indexExportSamples();
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "source_files", "generate",
                "-b", basePath.toString(),
                "-n", "file"};
        executeExpectSuccess(args);

        assertTrue(Files.exists(project.getSourceFilesMappingPath()));
    }

    @Test
    public void generateBasicMatchDryRunTest() throws Exception {
        indexExportSamples();
        Path srcPath1 = addSourceFile("276_182_E.tif");

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "source_files", "generate",
                "--dry-run",
                "-b", basePath.toString()};
        executeExpectSuccess(args);

        assertFalse(Files.exists(project.getSourceFilesMappingPath()));
        assertOutputContains("25,276_182_E.tif," + srcPath1 + ",");
        assertOutputContains("26,276_183_E.tif,,");
        assertOutputContains("27,276_203_E.tif,,");
    }

    @Test
    public void generateNestedPatternMatchDryRunTest() throws Exception {
        indexExportSamples();
        Path srcPath1 = addSourceFile("path/to/00276_op0182_0001_e.tif");
        Path srcPath3 = addSourceFile("00276_op0203_0001_e.tif");

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "source_files", "generate",
                "--dry-run",
                "-b", basePath.toString(),
                "-p", "(\\d+)\\_(\\d+)_E.tif",
                "-t", "00$1_op0$2_0001_e.tif" };
        executeExpectSuccess(args);

        assertFalse(Files.exists(project.getSourceFilesMappingPath()));
        assertOutputContains("25,276_182_E.tif," + srcPath1.toString() + ",");
        assertOutputContains("26,276_183_E.tif,,");
        assertOutputContains("27,276_203_E.tif," + srcPath3 + ",");
    }

    @Test
    public void generateUpdateAddSourceFileDryRunTest() throws Exception {
        indexExportSamples();
        Path srcPath1 = addSourceFile("276_182_E.tif");

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "source_files", "generate",
                "-b", basePath.toString()};
        executeExpectSuccess(args);

        Path srcPath2 = addSourceFile("276_183_E.tif");
        String[] args2 = new String[] {
                "-w", project.getProjectPath().toString(),
                "source_files", "generate",
                "-u",
                "--dry-run",
                "-b", basePath.toString()};
        executeExpectSuccess(args2);

        assertOutputContains("25,276_182_E.tif," + srcPath1 + ",");
        assertOutputContains("26,276_183_E.tif," + srcPath2 + ",");
        assertOutputContains("27,276_203_E.tif,,");
    }



    @Test
    public void validateValidTest() throws Exception {
        indexExportSamples();
        addSourceFile("276_182_E.tif");
        addSourceFile("276_183_E.tif");
        addSourceFile("276_203_E.tif");

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "source_files", "generate",
                "-b", basePath.toString()};
        executeExpectSuccess(args);

        String[] args2 = new String[] {
                "-w", project.getProjectPath().toString(),
                "source_files", "validate" };
        executeExpectSuccess(args2);

        assertOutputContains("PASS: Source file mapping at path " + project.getSourceFilesMappingPath() + " is valid");
    }

    @Test
    public void validateInvalidTest() throws Exception {
        indexExportSamples();
        addSourceFile("276_182_E.tif");

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "source_files", "generate",
                "-b", basePath.toString()};
        executeExpectSuccess(args);

        String[] args2 = new String[] {
                "-w", project.getProjectPath().toString(),
                "source_files", "validate" };
        executeExpectFailure(args2);

        assertOutputContains("FAIL: Source file mapping at path " + project.getSourceFilesMappingPath()
                + " is invalid");
        assertOutputContains("- No path mapped at line 3");
        assertOutputContains("- No path mapped at line 4");
        assertEquals(3, output.split("    - ").length, "Must only be two errors: " + output);
    }

    @Test
    public void statusValidTest() throws Exception {
        indexExportSamples();
        addSourceFile("276_182_E.tif");
        addSourceFile("276_183_E.tif");
        addSourceFile("276_203_E.tif");

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "source_files", "generate",
                "-b", basePath.toString()};
        executeExpectSuccess(args);

        String[] args2 = new String[] {
                "-w", project.getProjectPath().toString(),
                "source_files", "status" };
        executeExpectSuccess(args2);

        assertOutputMatches(".*Last Updated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Objects Mapped: +3 \\(100.0%\\).*");
        assertOutputMatches(".*Unmapped Objects: +0.*");
        assertOutputMatches(".*Mappings Valid: +Yes\n.*");
        assertOutputMatches(".*Potential Matches: +0.*");
    }

    @Test
    public void statusUnmappedVerboseTest() throws Exception {
        indexExportSamples();
        addSourceFile("276_182_E.tif");
        addSourceFile("276_203_E.tif");

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "source_files", "generate",
                "-b", basePath.toString()};
        executeExpectSuccess(args);

        String[] args2 = new String[] {
                "-w", project.getProjectPath().toString(),
                "source_files", "status",
                "-v" };
        executeExpectSuccess(args2);

        assertOutputMatches(".*Last Updated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Objects Mapped: +2 \\(66.7%\\).*");
        assertOutputMatches(".*Unmapped Objects: +1.*");
        assertOutputMatches(".*Unmapped Objects:.*\n + \\* 26.*");
        assertOutputMatches(".*Mappings Valid: +No.*");
        assertOutputMatches(".*Potential Matches: +0.*");
    }

    @Test
    public void statusUnmappedDoesNotContainGroupObjectsTest() throws Exception {
        indexGroupExportSamples();
        addSourceFile("276_185_E.tif");
        addSourceFile("276_203_E.tif");

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "source_files", "generate",
                "-b", basePath.toString()};
        executeExpectSuccess(args);

        String[] args2 = new String[] {
                "-w", project.getProjectPath().toString(),
                "group_mapping", "generate",
                "-n", "groupa"};
        executeExpectSuccess(args2);

        String[] args3 = new String[] {
                "-w", project.getProjectPath().toString(),
                "group_mapping", "sync" };
        executeExpectSuccess(args3);

        String[] args4 = new String[] {
                "-w", project.getProjectPath().toString(),
                "source_files", "status"};
        executeExpectSuccess(args4);

        assertOutputMatches(".*Unmapped Objects: +3.*");
    }

    @Test
    public void statusUnmappedDoesNotContainCompoundObjectsTest() throws Exception {
        indexCompoundExportSamples();

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "source_files", "generate",
                "-n", "filena",
                "-b", basePath.toString()};
        executeExpectSuccess(args);

        String[] args4 = new String[] {
                "-w", project.getProjectPath().toString(),
                "source_files", "status"};
        executeExpectSuccess(args4);

        assertOutputMatches(".*Unmapped Objects: +0.*");
    }

    @Test
    public void generateBlankSucceedsTest() throws Exception {
        indexExportSamples();
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "source_files", "generate",
                "-B"};
        executeExpectSuccess(args);

        assertTrue(Files.exists(project.getSourceFilesMappingPath()));
    }

    private void indexExportSamples() throws Exception {
        testHelper.indexExportData("mini_gilmer");
    }

    private void indexGroupExportSamples() throws Exception {
        testHelper.indexExportData("grouped_gilmer");
    }

    private void indexCompoundExportSamples() throws Exception {
        testHelper.indexExportData(Paths.get("src/test/resources/keepsakes_fields.csv"),"mini_keepsakes");
        addSourceFile("nccg_ck_09.tif");
        addSourceFile("nccg_ck_1042-22_v1.tif");
        addSourceFile("nccg_ck_1042-22_v2.tif");
        addSourceFile("nccg_ck_549-4_v1.tif");
        addSourceFile("nccg_ck_549-4_v2.tif");
    }

    private Path addSourceFile(String relPath) throws IOException {
        return testHelper.addSourceFile(relPath);
    }
}
