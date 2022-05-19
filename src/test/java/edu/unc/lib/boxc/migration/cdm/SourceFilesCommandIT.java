/**
 * Copyright 2008 The University of North Carolina at Chapel Hill
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.unc.lib.boxc.migration.cdm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;

import edu.unc.lib.boxc.migration.cdm.services.CdmFileRetrievalService;
import edu.unc.lib.boxc.migration.cdm.test.SipServiceHelper;
import org.junit.Before;
import org.junit.Test;

import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;

/**
 * @author bbpennel
 */
public class SourceFilesCommandIT extends AbstractCommandIT {
    private final static String COLLECTION_ID = "my_coll";

    private MigrationProject project;
    private Path basePath;
    private SipServiceHelper testHelper;

    @Before
    public void setup() throws Exception {
        project = MigrationProjectFactory.createMigrationProject(
                baseDir, COLLECTION_ID, null, USERNAME);
        basePath = tmpFolder.newFolder().toPath();
        testHelper = new SipServiceHelper(project, tmpFolder.getRoot().toPath());
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
        assertOutputContains("25,276_182_E.tif," + srcPath1.toString() + ",");
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

        assertOutputContains("25,276_182_E.tif," + srcPath1.toString() + ",");
        assertOutputContains("26,276_183_E.tif," + srcPath2.toString() + ",");
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
        assertEquals("Must only be two errors: " + output, 3, output.split("    - ").length);
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
}
