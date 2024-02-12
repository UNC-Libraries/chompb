package edu.unc.lib.boxc.migration.cdm;

import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author bbpennel
 */
public class AggregateFilesCommandIT extends AbstractCommandIT {
    private Path basePath;

    @BeforeEach
    public void setup() throws Exception {
        initProjectAndHelper();
        basePath = testHelper.getSourceFilesBasePath();
        Files.createDirectories(basePath);
    }

    @Test
    public void generateNoExportFieldTest() throws Exception {
        testHelper.indexExportData("mini_keepsakes");
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "aggregate_files", "generate",
                "-b", basePath.toString(),
                "-n", ""};
        executeExpectFailure(args);

        assertOutputContains("Must provide an export field");
    }

    @Test
    public void generateNoBasePathTest() throws Exception {
        testHelper.indexExportData("mini_keepsakes");
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "aggregate_files", "generate",
                "-n", "file"};
        executeExpectFailure(args);

        assertOutputContains("Must provide a base path");
    }

    @Test
    public void generateBasicMatchTopTest() throws Exception {
        testHelper.indexExportData("mini_keepsakes");
        Path srcPath1 = testHelper.addSourceFile("617.pdf");
        Path srcPath2 = testHelper.addSourceFile("620.pdf");
        executeExpectSuccess(argsGenerate("find"));

        assertTrue(Files.exists(project.getAggregateTopMappingPath()));
        assertFalse(Files.exists(project.getAggregateBottomMappingPath()));
        assertOutputMatches(".*Previous Files Mapped: +0.*");
        assertOutputMatches(".*New Files Mapped: +2.*");
        assertOutputMatches(".*Total Files Mapped: +2.*");
        assertOutputMatches(".*Total Files in Project: +5.*");
        assertOutputContains("604,617.cpd," + srcPath1);
        assertOutputContains("607,620.cpd," + srcPath2);
    }

    @Test
    public void generateBasicMatchDryRunTest() throws Exception {
        testHelper.indexExportData("mini_keepsakes");
        Path srcPath1 = testHelper.addSourceFile("617.pdf");
        Path srcPath2 = testHelper.addSourceFile("620.pdf");
        executeExpectSuccess(withDryRun(argsGenerate("find")));

        assertFalse(Files.exists(project.getAggregateTopMappingPath()));
        assertFalse(Files.exists(project.getAggregateBottomMappingPath()));
        assertOutputMatches(".*Previous Files Mapped: +0.*");
        assertOutputMatches(".*New Files Mapped: +2.*");
        assertOutputMatches(".*Total Files Mapped: +2.*");
        assertOutputMatches(".*Total Files in Project: +5.*");
        assertOutputContains("604,617.cpd," + srcPath1);
        assertOutputContains("607,620.cpd," + srcPath2);
    }

    @Test
    public void generateBasicMatchBottomTest() throws Exception {
        testHelper.indexExportData("mini_keepsakes");
        Path srcPath1 = testHelper.addSourceFile("617.pdf");
        Path srcPath2 = testHelper.addSourceFile("620.pdf");
        executeExpectSuccess(withSortBottom(argsGenerate("find")));

        assertFalse(Files.exists(project.getAggregateTopMappingPath()));
        assertTrue(Files.exists(project.getAggregateBottomMappingPath()));
        assertOutputMatches(".*Previous Files Mapped: +0.*");
        assertOutputMatches(".*New Files Mapped: +2.*");
        assertOutputMatches(".*Total Files Mapped: +2.*");
        assertOutputMatches(".*Total Files in Project: +5.*");
        assertOutputContains("604,617.cpd," + srcPath1);
        assertOutputContains("607,620.cpd," + srcPath2);
    }

    @Test
    public void generateBasicMatchBothTest() throws Exception {
        testHelper.indexExportData("mini_keepsakes");
        Path srcPath1 = testHelper.addSourceFile("617.pdf");
        Path srcPath2 = testHelper.addSourceFile("620.pdf");
        executeExpectSuccess(argsGenerate("find"));

        executeExpectSuccess(withSortBottom(argsGenerate("find")));

        assertTrue(Files.exists(project.getAggregateTopMappingPath()));
        assertTrue(Files.exists(project.getAggregateBottomMappingPath()));
        assertOutputMatches(".*Previous Files Mapped: +0.*");
        assertOutputMatches(".*New Files Mapped: +2.*");
        assertOutputMatches(".*Total Files Mapped: +2.*");
        assertOutputMatches(".*Total Files in Project: +5.*");
        assertOutputContains("604,617.cpd," + srcPath1);
        assertOutputContains("607,620.cpd," + srcPath2);
    }

    @Test
    public void validateTopValidTest() throws Exception {
        testHelper.indexExportData("mini_keepsakes");
        // Object 604 will have potential matches
        testHelper.addSourceFile("617.pdf");
        testHelper.addSourceFile("extra/617.pdf");
        // Object 607 will have two source matches
        testHelper.addSourceFile("620.pdf");
        testHelper.addSourceFile("607.pdf");

        executeExpectSuccess(argsGenerate("find"));
        executeExpectSuccess(withUpdate(argsGenerate(CdmFieldInfo.CDM_ID)));

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "aggregate_files", "validate" };
        executeExpectSuccess(args);

        assertOutputContains("PASS: Top aggregate file mappings at path " + project.getAggregateTopMappingPath() + " is valid");
    }

    @Test
    public void validateTopInvalidTest() throws Exception {
        testHelper.indexExportData("mini_keepsakes");
        var aggrPath1 = testHelper.addSourceFile("617.pdf");
        var aggrPath2 = testHelper.addSourceFile("650.pdf");
        executeExpectSuccess(argsGenerate("find"));

        // This should produce a duplicate
        Files.writeString(project.getAggregateTopMappingPath(),
                "\nsomeid,somefield," + aggrPath2 + "|" + aggrPath1 + ",",
                StandardOpenOption.APPEND);

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "aggregate_files", "validate" };
        executeExpectFailure(args);

        assertOutputContains("FAIL: Top aggregate file mappings at path " + project.getAggregateTopMappingPath()
                + " is invalid");
        assertOutputContains("- Duplicate mapping for path " + aggrPath1);
        assertEquals(2, output.split("    - ").length, "Must only be one error: " + output);
    }

    @Test
    public void validateBottomValidTest() throws Exception {
        testHelper.indexExportData("mini_keepsakes");
        // Object 604 will have potential matches
        testHelper.addSourceFile("617.pdf");
        testHelper.addSourceFile("620.pdf");

        executeExpectSuccess(withSortBottom(argsGenerate("find")));

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "aggregate_files", "validate",
                "--sort-bottom" };
        executeExpectSuccess(args);

        assertOutputContains("PASS: Bottom aggregate file mappings at path " + project.getAggregateBottomMappingPath() + " is valid");
    }

    @Test
    public void validateBottomInvalidTest() throws Exception {
        testHelper.indexExportData("mini_keepsakes");
        // Object 604 will have potential matches
        testHelper.addSourceFile("617.pdf");
        var aggrPath2 = testHelper.addSourceFile("620.pdf");
        executeExpectSuccess(withSortBottom(argsGenerate("find")));
        // Delete second file so that it will fail validation
        Files.delete(aggrPath2);

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "aggregate_files", "validate",
                "--sort-bottom" };
        executeExpectFailure(args);

        assertOutputContains("FAIL: Bottom aggregate file mappings at path " + project.getAggregateBottomMappingPath() + " is invalid due to the following issues");
        assertOutputContains("- Invalid path at line 3, file does not exist");
    }

    private List<String> argsGenerate(String field) {
        return new ArrayList(Arrays.asList(
                "-w", project.getProjectPath().toString(),
                "aggregate_files", "generate",
                "-b", basePath.toString(),
                "-p", "([^._]+)(\\.cpd)?$",
                "-t", "$1.pdf",
                "-n", field));
    }

    private List<String> withSortBottom(List<String> args) {
        args.add("--sort-bottom");
        return args;
    }

    private List<String> withUpdate(List<String> args) {
        args.add("--update");
        return args;
    }

    private List<String> withDryRun(List<String> args) {
        args.add("--dry-run");
        return args;
    }
}
