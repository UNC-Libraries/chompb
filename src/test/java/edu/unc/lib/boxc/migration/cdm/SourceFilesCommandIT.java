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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;

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

    @Before
    public void setup() throws Exception {
        project = MigrationProjectFactory.createMigrationProject(
                baseDir, COLLECTION_ID, null, USERNAME);
        basePath = tmpFolder.newFolder().toPath();
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
        assertOutputContains("26,276_183B_E.tif,,");
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
        assertOutputContains("26,276_183B_E.tif,,");
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

        Path srcPath2 = addSourceFile("276_183B_E.tif");
        String[] args2 = new String[] {
                "-w", project.getProjectPath().toString(),
                "source_files", "generate",
                "-u",
                "--dry-run",
                "-b", basePath.toString()};
        executeExpectSuccess(args2);

        assertOutputContains("25,276_182_E.tif," + srcPath1.toString() + ",");
        assertOutputContains("26,276_183B_E.tif," + srcPath2.toString() + ",");
        assertOutputContains("27,276_203_E.tif,,");
    }

    private void indexExportSamples() throws Exception {
        Files.createDirectories(project.getExportPath());
        Files.copy(Paths.get("src/test/resources/sample_exports/export_1.xml"),
                project.getExportPath().resolve("export_all.xml"));
        Files.copy(Paths.get("src/test/resources/gilmer_fields.csv"), project.getFieldsPath());

        project.getProjectProperties().setExportedDate(Instant.now());
        ProjectPropertiesSerialization.write(project);
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "index"};
        executeExpectSuccess(args);
    }

    private Path addSourceFile(String relPath) throws IOException {
        Path srcPath = basePath.resolve(relPath);
        // Create parent directories in case they don't exist
        Files.createDirectories(srcPath.getParent());
        Files.createFile(srcPath);
        return srcPath;
    }
}