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

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;

import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;

/**
 * @author bbpennel
 */
public class DescriptionsCommandIT extends AbstractCommandIT {
    private final static String COLLECTION_ID = "my_coll";

    private MigrationProject project;

    @Before
    public void setup() throws Exception {
        project = MigrationProjectFactory.createMigrationProject(
                baseDir, COLLECTION_ID, null, USERNAME);
    }

    @Test
    public void expandDescriptions() throws Exception {
        setIndexedDate();

        Files.copy(Paths.get("src/test/resources/mods_collections/gilmer_mods1.xml"),
                project.getDescriptionsPath().resolve("gilmer_mods1.xml"));

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "descriptions", "expand" };
        executeExpectSuccess(args);
        assertOutputContains("Descriptions expanded to 3 separate files");

        assertExpandedDescriptionFilesCount(3);
    }

    @Test
    public void expandNoDescriptionsFiles() throws Exception {
        setIndexedDate();

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "descriptions", "expand" };
        executeExpectSuccess(args);
        assertOutputContains("Descriptions expanded to 0 separate files");

        assertFalse(Files.exists(project.getExpandedDescriptionsPath()));
    }

    @Test
    public void expandBadInputFile() throws Exception {
        Files.createFile(project.getDescriptionsPath().resolve("input.xml"));

        setIndexedDate();

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "descriptions", "expand" };
        executeExpectFailure(args);
        assertOutputContains("ParseError");

        assertFalse(Files.exists(project.getExpandedDescriptionsPath()));
    }

    private void assertExpandedDescriptionFilesCount(int expected) throws Exception {
        int fileCount = 0;
        try (DirectoryStream<Path> nested = Files.newDirectoryStream(project.getExpandedDescriptionsPath())) {
            for (Path nestedDir : nested) {
                try (Stream<Path> files = Files.list(nestedDir)) {
                    fileCount += files.count();
                }
            }
        }
        assertEquals("Unexpected number of expanded MODS files", expected, fileCount);
    }

    private void setIndexedDate() throws Exception {
        project.getProjectProperties().setIndexedDate(Instant.now());
        ProjectPropertiesSerialization.write(project);
    }
}
