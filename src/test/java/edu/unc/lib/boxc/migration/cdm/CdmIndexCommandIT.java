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

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProjectProperties;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;

/**
 * @author bbpennel
 */
public class CdmIndexCommandIT extends AbstractCommandIT {
    private final static String COLLECTION_ID = "my_coll";

    private MigrationProject project;

    @Test
    public void indexMultipleFilesTest() throws Exception {
        project = MigrationProjectFactory.createMigrationProject(
                baseDir, COLLECTION_ID, null, USERNAME);
        Files.createDirectories(project.getExportPath());

        Files.copy(Paths.get("src/test/resources/sample_exports/export_1.xml"),
                project.getExportPath().resolve("export_1.xml"));
        Files.copy(Paths.get("src/test/resources/sample_exports/export_2.xml"),
                project.getExportPath().resolve("export_2.xml"));
        Files.copy(Paths.get("src/test/resources/gilmer_fields.csv"), project.getFieldsPath());
        setExportedDate();

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "index"};
        executeExpectSuccess(args);

        assertTrue(Files.exists(project.getIndexPath()));
        assertDateIndexedPresent();
    }

    @Test
    public void indexAlreadyExistsTest() throws Exception {
        project = MigrationProjectFactory.createMigrationProject(
                baseDir, COLLECTION_ID, null, USERNAME);
        Files.createDirectories(project.getExportPath());

        Files.copy(Paths.get("src/test/resources/sample_exports/export_1.xml"),
                project.getExportPath().resolve("export_1.xml"));
        Files.copy(Paths.get("src/test/resources/gilmer_fields.csv"), project.getFieldsPath());
        setExportedDate();

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "index"};
        executeExpectSuccess(args);
        assertTrue(Files.exists(project.getIndexPath()));
        long indexSize = Files.size(project.getIndexPath());

        // Index a second time, should fail and index should be unchanged
        executeExpectFailure(args);
        assertOutputContains("Cannot create index, an index file already exists");
        assertTrue(Files.exists(project.getIndexPath()));
        assertEquals(indexSize, Files.size(project.getIndexPath()));
        assertDateIndexedPresent();

        // Add more data and index again with force flag
        Files.copy(Paths.get("src/test/resources/sample_exports/export_2.xml"),
                project.getExportPath().resolve("export_2.xml"));

        String[] argsForce = new String[] {
                "-w", project.getProjectPath().toString(),
                "index",
                "-f"};
        executeExpectSuccess(argsForce);
        assertTrue(Files.exists(project.getIndexPath()));
        assertNotEquals("Index should have changed size", indexSize, Files.size(project.getIndexPath()));
        assertDateIndexedPresent();
    }

    @Test
    public void indexingFailureTest() throws Exception {
        project = MigrationProjectFactory.createMigrationProject(
                baseDir, COLLECTION_ID, null, USERNAME);
        Files.createDirectories(project.getExportPath());

        Files.copy(Paths.get("src/test/resources/sample_exports/export_1.xml"),
                project.getExportPath().resolve("export_1.xml"));
        FileUtils.write(project.getExportPath().resolve("export_2.xml").toFile(), "uh oh", ISO_8859_1);
        Files.copy(Paths.get("src/test/resources/gilmer_fields.csv"), project.getFieldsPath());
        setExportedDate();

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "index"};
        executeExpectFailure(args);
        assertOutputContains("Failed to parse export file");
        assertDateIndexedNotPresent();
        assertTrue("Index file should be cleaned up", Files.notExists(project.getIndexPath()));
    }

    private void setExportedDate() throws Exception {
        project.getProjectProperties().setExportedDate(Instant.now());
        ProjectPropertiesSerialization.write(project);
    }

    private void assertDateIndexedPresent() throws Exception {
        MigrationProjectProperties props = ProjectPropertiesSerialization.read(project.getProjectPropertiesPath());
        assertNotNull(props.getIndexedDate());
    }

    private void assertDateIndexedNotPresent() throws Exception {
        MigrationProjectProperties props = ProjectPropertiesSerialization.read(project.getProjectPropertiesPath());
        assertNull(props.getIndexedDate());
    }
}
