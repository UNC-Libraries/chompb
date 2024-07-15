package edu.unc.lib.boxc.migration.cdm.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;

import edu.unc.lib.boxc.migration.cdm.test.BxcEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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

    @TempDir
    public Path tmpFolder;
    private Path projectsBase;
    private String testEnv = CdmEnvironmentHelper.DEFAULT_ENV_ID;
    private String bxcTestEnv = BxcEnvironmentHelper.DEFAULT_ENV_ID;

    @BeforeEach
    public void setup() throws Exception {
        projectsBase = tmpFolder;
    }

    @Test
    public void createNoUserTest() throws Exception {
        try {
            MigrationProjectFactory.createCdmMigrationProject(projectsBase, null, null, null,
                    testEnv, bxcTestEnv);
            fail();
        } catch (IllegalArgumentException e) {
            // Expected
        }
        assertTrue(Files.notExists(projectsBase.resolve(PROJ_NAME)));
    }

    @Test
    public void createWithNameTest() throws Exception {
        MigrationProject project = MigrationProjectFactory
                .createCdmMigrationProject(projectsBase, PROJ_NAME, null, USERNAME, testEnv, bxcTestEnv);

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
                .createCdmMigrationProject(projectsBase, PROJ_NAME, null, USERNAME, testEnv, bxcTestEnv);

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
            MigrationProjectFactory.createCdmMigrationProject(projectsBase, PROJ_NAME, null, USERNAME, testEnv, bxcTestEnv);
            fail();
        } catch (InvalidProjectStateException e) {
            assertTrue(e.getMessage().contains("already exists and is not a directory"));
        }
    }

    @Test
    public void createWithNameAndCollectionTest() throws Exception {
        MigrationProject project = MigrationProjectFactory
                .createCdmMigrationProject(projectsBase, PROJ_NAME, COLL_ID, USERNAME, testEnv, bxcTestEnv);

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
                .createCdmMigrationProject(projectPath, null, null, USERNAME, testEnv, bxcTestEnv);

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
                .createCdmMigrationProject(projectPath, null, COLL_ID, USERNAME, testEnv, bxcTestEnv);

        assertNotNull(project);
        assertEquals(projectPath, project.getProjectPath());

        assertReturnedPropertiesPopulated(project, PROJ_NAME, COLL_ID);
        assertPropertiesFilePopulated(project, PROJ_NAME, COLL_ID);
    }

    @Test
    public void createProjectAlreadyExistsTest() throws Exception {
        MigrationProject project = MigrationProjectFactory
                .createCdmMigrationProject(projectsBase, PROJ_NAME, null, USERNAME, testEnv, bxcTestEnv);

        try {
            // Create file at expected project path
            MigrationProjectFactory.createCdmMigrationProject(projectsBase, PROJ_NAME, COLL_ID, USERNAME, testEnv, bxcTestEnv);
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
            assertFalse(dirStream.iterator().hasNext(), "Existing directory should remain empty");
        }
    }

    @Test
    public void loadValidProject() throws Exception {
        Path projectPath = projectsBase.resolve(PROJ_NAME);
        MigrationProject projectCreated = MigrationProjectFactory
                .createCdmMigrationProject(projectsBase, PROJ_NAME, COLL_ID, USERNAME, testEnv, bxcTestEnv);

        MigrationProject projectLoaded = MigrationProjectFactory.loadMigrationProject(projectPath);

        assertNotNull(projectLoaded);
        assertEquals(projectPath, projectLoaded.getProjectPath());

        assertReturnedPropertiesPopulated(projectLoaded, PROJ_NAME, COLL_ID);
        assertPropertiesFilePopulated(projectLoaded, PROJ_NAME, COLL_ID);

        assertEquals(projectCreated.getProjectProperties().getCreatedDate(),
                projectLoaded.getProjectProperties().getCreatedDate(),
                "Expect created and loaded projects to have same timestamp");
    }

    private void assertPropertiesFilePopulated(MigrationProject project, String expName, String expCollId)
            throws IOException {
        Path propertiesPath = project.getProjectPropertiesPath();
        assertTrue(Files.exists(propertiesPath), "Properties object must exist");
        MigrationProjectProperties properties = ProjectPropertiesSerialization.read(propertiesPath);
        assertPropertiesSet(properties, expName, expCollId);
    }

    private void assertReturnedPropertiesPopulated(MigrationProject project, String expName, String expCollId) {
        assertPropertiesSet(project.getProjectProperties(), expName, expCollId);
    }

    private void assertPropertiesSet(MigrationProjectProperties properties, String expName, String expCollId) {
        assertEquals(USERNAME, properties.getCreator());
        assertEquals(expName, properties.getName(), "Project name did not match expected value");
        assertEquals(expCollId, properties.getCdmCollectionId(), "CDM Collection ID did not match expected value");
        assertNotNull(properties.getCreatedDate(), "Created date not set");
        assertEquals("test", properties.getCdmEnvironmentId());
    }
}
