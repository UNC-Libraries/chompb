package edu.unc.lib.boxc.migration.cdm;

import edu.unc.lib.boxc.migration.cdm.jobs.VelocicroptorRemoteJob;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.ArchiveProjectsService;
import edu.unc.lib.boxc.migration.cdm.services.ListProjectsService;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.test.BxcEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.Collections;

public class ListProjectsCommandIT extends AbstractCommandIT {
    private static final String PROJECT_ID_2 = "proj2";

    @BeforeEach
    public void setup() throws Exception {
        initProjectAndHelper();
        setupChompbConfig();
    }

    @Test
    public void invalidDirectoryTest() throws Exception {
        String[] args = new String[] {
                "-w", String.valueOf(Path.of("test")),
                "list_projects" };
        executeExpectFailure(args);

        assertOutputContains("does not exist");
    }

    @Test
    public void listProjectTest() throws Exception {
        File velocicroptorFile = new File(project.getProjectPath() + "/"
                + VelocicroptorRemoteJob.RESULTS_REL_PATH + "/job_complete");
        velocicroptorFile.mkdirs();

        String[] args = new String[] {
                "-w", String.valueOf(baseDir),
                "list_projects" };
        executeExpectSuccess(args);

        assertOutputContains("\"" + ListProjectsService.PROJECT_PATH + "\" : \"" + Path.of(baseDir
                + "/" + PROJECT_ID) + "\"");
        assertOutputContains("\"" + ListProjectsService.STATUS + "\" : \"initialized\"");
        assertOutputContains("\"" + ListProjectsService.ALLOWED_ACTIONS + "\" : [ ]");
        assertOutputContains("\"name\" : \"" + PROJECT_ID + "\"");
        assertOutputContains("\"" + ListProjectsService.PROCESSING_JOBS + "\"");
        assertOutputContains("\"" + VelocicroptorRemoteJob.JOB_NAME + "\"");
        assertOutputContains("\"" + ListProjectsService.STATUS + "\" : \""
                + ListProjectsService.COMPLETED + "\"");
    }

    @Test
    public void listArchivedProjectTest() throws Exception {
        ArchiveProjectsService archiveProjectsService = new ArchiveProjectsService();
        archiveProjectsService.archiveProjects(tmpFolder, Collections.singletonList(PROJECT_ID));

        String[] args = new String[] {
                "-w", String.valueOf(baseDir),
                "list_projects",
                "--include-archived"};
        executeExpectSuccess(args);

        assertOutputContains("\"" + ListProjectsService.PROJECT_PATH + "\" : \""
                + baseDir.resolve(ArchiveProjectsService.ARCHIVED+ "/" + PROJECT_ID) + "\"");
        assertOutputContains("\"" + ListProjectsService.STATUS + "\" : \"archived\"");
        assertOutputContains("\"" + ListProjectsService.ALLOWED_ACTIONS + "\" : [ ]");
        assertOutputContains("\"name\" : \"" + PROJECT_ID + "\"");
    }

    @Test
    public void listMultipleProjectsTest() throws Exception {
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder, PROJECT_ID_2, null, "user", CdmEnvironmentHelper.DEFAULT_ENV_ID,
                BxcEnvironmentHelper.DEFAULT_ENV_ID, MigrationProject.PROJECT_SOURCE_CDM);

        String[] args = new String[] {
                "-w", String.valueOf(baseDir),
                "list_projects" };
        executeExpectSuccess(args);

        assertOutputContains("\"" + ListProjectsService.PROJECT_PATH + "\" : \"" +
                Path.of(baseDir + "/" + PROJECT_ID) + "\"");
        assertOutputContains("\"" + ListProjectsService.PROJECT_PATH + "\" : \"" +
                Path.of(baseDir + "/" + PROJECT_ID_2) + "\"");
        assertOutputContains("\""+ ListProjectsService.STATUS + "\" : \"initialized\"");
        assertOutputContains("\"" + ListProjectsService.ALLOWED_ACTIONS + "\" : [ ]");
        assertOutputContains("\"name\" : \"" + PROJECT_ID + "\"");
        assertOutputContains("\"name\" : \"" + PROJECT_ID_2 + "\"");
    }
}
