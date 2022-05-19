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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;

import edu.unc.lib.boxc.migration.cdm.test.SipServiceHelper;
import org.junit.Before;
import org.junit.Test;

import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProjectProperties;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;

/**
 * @author bbpennel
 */
public class AccessFilesCommandIT extends AbstractCommandIT {
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
        assertFalse(Files.exists(project.getSourceFilesMappingPath()));

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
        assertTrue(output.contains("25,276_182_E.tif," + srcPath1.toString() + ","));
        assertTrue(output.contains("26,276_183_E.tif,,"));
        assertTrue(output.contains("27,276_203_E.tif,,"));

        assertUpdatedDateNotPresent();
    }

    @Test
    public void generateNestedPatternMatchDryRunTest() throws Exception {
        indexExportSamples();
        Path srcPath1 = addSourceFile("path/to/00276_op0182_0001_e.tif");
        Path srcPath3 = addSourceFile("00276_op0203_0001_e.tif");

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "access_files", "generate",
                "--dry-run",
                "-b", basePath.toString(),
                "-p", "(\\d+)\\_(\\d+)_E.tif",
                "-t", "00$1_op0$2_0001_e.tif" };
        executeExpectSuccess(args);

        assertFalse(Files.exists(project.getAccessFilesMappingPath()));
        assertTrue(output.contains("25,276_182_E.tif," + srcPath1.toString() + ","));
        assertTrue(output.contains("26,276_183_E.tif,,"));
        assertTrue(output.contains("27,276_203_E.tif," + srcPath3 + ","));

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
        assertNotNull("Updated timestamp must be set", props.getAccessFilesUpdatedDate());
        assertNull("Source mapping timestamp must not be set", props.getSourceFilesUpdatedDate());
    }

    private void assertUpdatedDateNotPresent() throws Exception {
        MigrationProjectProperties props = ProjectPropertiesSerialization.read(project.getProjectPropertiesPath());
        assertNull("Updated timestamp must not be set", props.getAccessFilesUpdatedDate());
        assertNull("Source mapping timestamp must not be set", props.getSourceFilesUpdatedDate());
    }
}
