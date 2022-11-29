package edu.unc.lib.boxc.migration.cdm;

import edu.unc.lib.boxc.migration.cdm.services.SipService;
import edu.unc.lib.boxc.migration.cdm.test.RedirectMappingHelper;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * @author snluong
 */
public class IndexRedirectCommandIT  extends AbstractCommandIT {
    private static final String DEST_UUID = "7a33f5e6-f0ca-461c-8df0-c76c62198b17";
    private SipService sipsService;
    private RedirectMappingHelper redirectMappingHelper;
    private Path propertiesPath;

    @Before
    public void setup() throws Exception {
        initProjectAndHelper();

        Files.createDirectories(project.getExportPath());
        sipsService = testHelper.createSipsService();
        redirectMappingHelper = new RedirectMappingHelper(project);
        redirectMappingHelper.createRedirectMappingsTableInDb();
        propertiesPath = redirectMappingHelper.createDbConnectionPropertiesFile(tmpFolder, "sqlite");
    }

    @Test
    public void indexRedirectsFailsIfProjectNotSubmitted() {
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "index_redirects",
                "--db-connection", propertiesPath.toString()};
        executeExpectFailure(args);

        assertOutputContains("Must submit the collection prior to indexing");
    }

    @Test
    public void indexRedirectsSucceedsIfProjectIsSubmitted() throws Exception {
        testHelper.initializeDefaultProjectState(DEST_UUID);
        sipsService.generateSips(redirectMappingHelper.makeOptions());
        testHelper.addSipsSubmitted();
        ProjectPropertiesSerialization.write(project);

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "index_redirects",
                "--db-connection", propertiesPath.toString()};
        executeExpectSuccess(args);
        assertOutputContains("Redirect mapping indexing completed. Yay!");
    }

    @Test
    public void indexRedirectsFailsIfDbConnectionStringNotIncluded() {
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "index_redirects"};
        executeExpectFailure(args);

        assertOutputContains("The DB connection path must be included");
    }
}
