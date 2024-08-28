package edu.unc.lib.boxc.migration.cdm;

import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.ListProjectsService;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.test.BxcEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

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
    public void listProjectsTest() throws Exception {
        String[] args = new String[] {
                "-w", String.valueOf(baseDir),
                "list_projects" };
        executeExpectSuccess(args);

        assertOutputContains("\"" + ListProjectsService.PROJECT_PATH + "\" : \"" + baseDir + "\"");
        assertOutputContains("\"" + ListProjectsService.STATUS + "\" : \"initialized\"");
        assertOutputContains("\"" + ListProjectsService.ALLOWED_ACTIONS + "\" : null");
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

        assertOutputContains("\"" + ListProjectsService.PROJECT_PATH + "\" : \"" + baseDir + "\"");
        assertOutputContains("\""+ ListProjectsService.STATUS + "\" : \"initialized\"");
        assertOutputContains("\"" + ListProjectsService.ALLOWED_ACTIONS + "\" : null");
        assertOutputContains("\"name\" : \"" + PROJECT_ID + "\"");
        assertOutputContains("\"name\" : \"" + PROJECT_ID_2 + "\"");
    }
}
