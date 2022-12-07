package edu.unc.lib.boxc.migration.cdm;

import edu.unc.lib.boxc.migration.cdm.services.MigrationTypeReportService;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.Assert.assertFalse;

/**
 * @author krwong
 */
public class MigrationTypeReportCommandIT extends AbstractCommandIT {
    private MigrationTypeReportService typeReportService;

    @Before
    public void setup() throws Exception {
        typeReportService = new MigrationTypeReportService();
    }

    @Test
    public void generateMigrationTypeReportTest() throws Exception {
        initProject();
        Files.copy(Paths.get("src/test/resources/post_migration_report.csv"), project.getPostMigrationReportPath());

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "report_migration_types" };
        executeExpectSuccess(args);

        assertOutputContains("Number of Works: 1");
        assertOutputContains("Number of Files: 1");
    }

    @Test
    public void noReportTest() throws Exception {
        initProject();
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "report_migration_types" };
        executeExpectFailure(args);

        assertOutputContains("Cannot generate migration types report. Post migration report not found");
        assertFalse(Files.exists(project.getPostMigrationReportPath()));
    }
}
