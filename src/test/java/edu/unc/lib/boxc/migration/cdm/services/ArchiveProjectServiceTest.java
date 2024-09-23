package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.test.BxcEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.SipServiceHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.MockitoAnnotations.openMocks;

public class ArchiveProjectServiceTest {
    private static final String PROJECT_NAME = "proj";
    private static final String PROJECT_NAME_2 = "proj2";

    @TempDir
    public Path tmpFolder;

    private SipServiceHelper testHelper;
    private MigrationProject project;
    private ArchiveProjectService service;

    private AutoCloseable closeable;

    @BeforeEach
    public void setup() throws Exception {
        closeable = openMocks(this);
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder, PROJECT_NAME, null, "user", CdmEnvironmentHelper.DEFAULT_ENV_ID,
                BxcEnvironmentHelper.DEFAULT_ENV_ID, MigrationProject.PROJECT_SOURCE_CDM);
        testHelper = new SipServiceHelper(project, tmpFolder);

        service = new ArchiveProjectService();
    }

    @AfterEach
    void closeService() throws Exception {
        closeable.close();
    }

    @Test
    public void archiveProjectTest() throws Exception {
        List<Path> testProjects = new ArrayList<>();
        testProjects.add(tmpFolder.resolve(PROJECT_NAME));
        service.archiveProject(tmpFolder, testProjects);

        assertTrue(Files.exists(tmpFolder.resolve("archived/" + PROJECT_NAME)));
    }

    @Test
    public void archiveInvalidProjectTest() throws Exception {
        try {
            List<Path> testProjects = new ArrayList<>();
            testProjects.add(tmpFolder.resolve(PROJECT_NAME_2));
            service.archiveProject(tmpFolder, testProjects);
            fail();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Migration project " + tmpFolder.resolve(PROJECT_NAME_2)
                    + " does not exist"));
        }
    }

    @Test
    public void archiveMultipleProjectsTest() throws Exception {
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder, PROJECT_NAME_2, null, "user", CdmEnvironmentHelper.DEFAULT_ENV_ID,
                BxcEnvironmentHelper.DEFAULT_ENV_ID, MigrationProject.PROJECT_SOURCE_CDM);

        List<Path> testProjects = new ArrayList<>();
        testProjects.add(tmpFolder.resolve(PROJECT_NAME));
        testProjects.add(tmpFolder.resolve(PROJECT_NAME_2));
        service.archiveProject(tmpFolder, testProjects);

        assertTrue(Files.exists(tmpFolder.resolve("archived/" + PROJECT_NAME)));
        assertTrue(Files.exists(tmpFolder.resolve("archived/" + PROJECT_NAME_2)));
    }
}
