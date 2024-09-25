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

public class ArchiveProjectsServiceTest {
    private static final String PROJECT_NAME = "proj";
    private static final String PROJECT_NAME_2 = "proj2";

    @TempDir
    public Path tmpFolder;

    private SipServiceHelper testHelper;
    private MigrationProject project;
    private ArchiveProjectsService service;

    private AutoCloseable closeable;

    @BeforeEach
    public void setup() throws Exception {
        closeable = openMocks(this);
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder, PROJECT_NAME, null, "user", CdmEnvironmentHelper.DEFAULT_ENV_ID,
                BxcEnvironmentHelper.DEFAULT_ENV_ID, MigrationProject.PROJECT_SOURCE_CDM);
        testHelper = new SipServiceHelper(project, tmpFolder);

        service = new ArchiveProjectsService();
    }

    @AfterEach
    void closeService() throws Exception {
        closeable.close();
    }

    @Test
    public void archiveProjectTest() throws Exception {
        List<String> testProjects = new ArrayList<>();
        testProjects.add(PROJECT_NAME);
        service.archiveProjects(tmpFolder, testProjects);

        assertTrue(Files.exists(tmpFolder.resolve(service.ARCHIVED + "/" + PROJECT_NAME)));
    }

    @Test
    public void archiveInvalidProjectTest() throws Exception {
        try {
            List<String> testProjects = new ArrayList<>();
            testProjects.add(PROJECT_NAME_2);
            service.archiveProjects(tmpFolder, testProjects);
            fail();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Migration project " + PROJECT_NAME_2 + " does not exist"));
        }
    }

    @Test
    public void archiveEmptyProjectTest() throws Exception {
        try {
            List<String> testProjects = new ArrayList<>();;
            service.archiveProjects(tmpFolder, testProjects);
            fail();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Project names cannot be empty"));
        }
    }

    @Test
    public void archiveMultipleProjectsTest() throws Exception {
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder, PROJECT_NAME_2, null, "user", CdmEnvironmentHelper.DEFAULT_ENV_ID,
                BxcEnvironmentHelper.DEFAULT_ENV_ID, MigrationProject.PROJECT_SOURCE_CDM);

        List<String> testProjects = new ArrayList<>();
        testProjects.add(PROJECT_NAME);
        testProjects.add(PROJECT_NAME_2);
        service.archiveProjects(tmpFolder, testProjects);

        assertTrue(Files.exists(tmpFolder.resolve(service.ARCHIVED + "/" + PROJECT_NAME)));
        assertTrue(Files.exists(tmpFolder.resolve(service.ARCHIVED + "/" + PROJECT_NAME_2)));
    }
}
