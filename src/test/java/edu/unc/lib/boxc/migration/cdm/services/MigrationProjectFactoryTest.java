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
package edu.unc.lib.boxc.migration.cdm.services;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;

import edu.unc.lib.boxc.migration.cdm.model.CdmEnvironment;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProjectProperties;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;

/**
 * @author bbpennel
 */
public class MigrationProjectFactoryTest {
    private static final String USERNAME = "cdmuser";
    private static final String PROJ_NAME = "myproject";
    private static final String COLL_ID = "coll_12345";

    @Rule
    public final TemporaryFolder tmpFolder = new TemporaryFolder();
    private Path projectsBase;
    private String testEnv = CdmEnvironmentHelper.DEFAULT_ENV;

    @Before
    public void setup() throws Exception {
        tmpFolder.create();
        projectsBase = tmpFolder.getRoot().toPath();
    }

    @Test
    public void createNoUserTest() throws Exception {
        try {
            MigrationProjectFactory.createMigrationProject(projectsBase, null, null, null, testEnv);
            fail();
        } catch (IllegalArgumentException e) {
            // Expected
        }
        assertTrue(Files.notExists(projectsBase.resolve(PROJ_NAME)));
    }

    @Test
    public void createWithNameTest() throws Exception {
        MigrationProject project = MigrationProjectFactory
                .createMigrationProject(projectsBase, PROJ_NAME, null, USERNAME, testEnv);

        assertNotNull(project);
        assertEquals(projectsBase.resolve(PROJ_NAME), project.getProjectPath());

        assertReturnedPropertiesPopulated(project, PROJ_NAME, PROJ_NAME);
        assertPropertiesFilePopulated(project, PROJ_NAME, PROJ_NAME);
    }

    @Test
    public void createWithNameAlreadyExistsTest() throws Exception {
        // Create project directory ahead of time
        Files.createDirectory(projectsBase.resolve(PROJ_NAME));

        MigrationProject project = MigrationProjectFactory
                .createMigrationProject(projectsBase, PROJ_NAME, null, USERNAME, testEnv);

        assertNotNull(project);
        assertEquals(projectsBase.resolve(PROJ_NAME), project.getProjectPath());

        assertReturnedPropertiesPopulated(project, PROJ_NAME, PROJ_NAME);
        assertPropertiesFilePopulated(project, PROJ_NAME, PROJ_NAME);
    }

    @Test
    public void createPathNotDirectoryTest() throws Exception {
        Files.createFile(projectsBase.resolve(PROJ_NAME));
        try {
            // Create file at expected project path
            MigrationProjectFactory.createMigrationProject(projectsBase, PROJ_NAME, null, USERNAME, testEnv);
            fail();
        } catch (InvalidProjectStateException e) {
            assertTrue(e.getMessage().contains("already exists and is not a directory"));
        }
    }

    @Test
    public void createWithNameAndCollectionTest() throws Exception {
        MigrationProject project = MigrationProjectFactory
                .createMigrationProject(projectsBase, PROJ_NAME, COLL_ID, USERNAME, testEnv);

        assertNotNull(project);
        assertEquals(projectsBase.resolve(PROJ_NAME), project.getProjectPath());

        assertReturnedPropertiesPopulated(project, PROJ_NAME, COLL_ID);
        assertPropertiesFilePopulated(project, PROJ_NAME, COLL_ID);
    }

    @Test
    public void createForExistingDirectoryTest() throws Exception {
        // Create project directory ahead of time
        Path projectPath = projectsBase.resolve(PROJ_NAME);
        Files.createDirectory(projectPath);

        MigrationProject project = MigrationProjectFactory
                .createMigrationProject(projectPath, null, null, USERNAME, testEnv);

        assertNotNull(project);
        assertEquals(projectPath, project.getProjectPath());

        assertReturnedPropertiesPopulated(project, PROJ_NAME, PROJ_NAME);
        assertPropertiesFilePopulated(project, PROJ_NAME, PROJ_NAME);
    }

    @Test
    public void createForExistingDirectoryWithCollIdTest() throws Exception {
        // Create project directory ahead of time
        Path projectPath = projectsBase.resolve(PROJ_NAME);
        Files.createDirectory(projectPath);

        MigrationProject project = MigrationProjectFactory
                .createMigrationProject(projectPath, null, COLL_ID, USERNAME, testEnv);

        assertNotNull(project);
        assertEquals(projectPath, project.getProjectPath());

        assertReturnedPropertiesPopulated(project, PROJ_NAME, COLL_ID);
        assertPropertiesFilePopulated(project, PROJ_NAME, COLL_ID);
    }

    @Test
    public void createProjectAlreadyExistsTest() throws Exception {
        MigrationProject project = MigrationProjectFactory
                .createMigrationProject(projectsBase, PROJ_NAME, null, USERNAME, testEnv);

        try {
            // Create file at expected project path
            MigrationProjectFactory.createMigrationProject(projectsBase, PROJ_NAME, COLL_ID, USERNAME, testEnv);
            fail();
        } catch (InvalidProjectStateException e) {
            assertTrue(e.getMessage().contains("directory already contains a migration project"));
        }

        // Original project should still exist and be unchanged
        assertEquals(projectsBase.resolve(PROJ_NAME), project.getProjectPath());

        assertReturnedPropertiesPopulated(project, PROJ_NAME, PROJ_NAME);
        assertPropertiesFilePopulated(project, PROJ_NAME, PROJ_NAME);
    }

    @Test
    public void loadPathNotExistTest() throws Exception {
        try {
            MigrationProjectFactory.loadMigrationProject(projectsBase.resolve(PROJ_NAME));
            fail();
        } catch (InvalidProjectStateException e) {
            assertTrue(e.getMessage().contains("does not exist"));
        }
        assertTrue(Files.notExists(projectsBase.resolve(PROJ_NAME)));
    }

    @Test
    public void loadNotAProjectTest() throws Exception {
        Path projectPath = projectsBase.resolve(PROJ_NAME);
        Files.createDirectory(projectPath);
        try {
            MigrationProjectFactory.loadMigrationProject(projectPath);
            fail();
        } catch (InvalidProjectStateException e) {
            assertTrue(e.getMessage().contains("does not contain an initialized project"));
        }
        assertTrue(Files.exists(projectPath));
        try(DirectoryStream<Path> dirStream = Files.newDirectoryStream(projectPath)) {
            assertFalse("Existing directory should remain empty", dirStream.iterator().hasNext());
        }
    }

    @Test
    public void loadValidProject() throws Exception {
        Path projectPath = projectsBase.resolve(PROJ_NAME);
        MigrationProject projectCreated = MigrationProjectFactory
                .createMigrationProject(projectsBase, PROJ_NAME, COLL_ID, USERNAME, testEnv);

        MigrationProject projectLoaded = MigrationProjectFactory.loadMigrationProject(projectPath);

        assertNotNull(projectLoaded);
        assertEquals(projectPath, projectLoaded.getProjectPath());

        assertReturnedPropertiesPopulated(projectLoaded, PROJ_NAME, COLL_ID);
        assertPropertiesFilePopulated(projectLoaded, PROJ_NAME, COLL_ID);

        assertEquals("Expect created and loaded projects to have same timestamp",
                projectCreated.getProjectProperties().getCreatedDate(),
                projectLoaded.getProjectProperties().getCreatedDate());
    }

    private void assertPropertiesFilePopulated(MigrationProject project, String expName, String expCollId)
            throws IOException {
        Path propertiesPath = project.getProjectPropertiesPath();
        assertTrue("Properties object must exist", Files.exists(propertiesPath));
        MigrationProjectProperties properties = ProjectPropertiesSerialization.read(propertiesPath);
        assertPropertiesSet(properties, expName, expCollId);
    }

    private void assertReturnedPropertiesPopulated(MigrationProject project, String expName, String expCollId) {
        assertPropertiesSet(project.getProjectProperties(), expName, expCollId);
    }

    private void assertPropertiesSet(MigrationProjectProperties properties, String expName, String expCollId) {
        assertEquals(USERNAME, properties.getCreator());
        assertEquals("Project name did not match expected value", expName, properties.getName());
        assertEquals("CDM Collection ID did not match expected value", expCollId, properties.getCdmCollectionId());
        assertNotNull("Created date not set", properties.getCreatedDate());
        assertEquals("test", properties.getCdmEnvironment());
    }
}
