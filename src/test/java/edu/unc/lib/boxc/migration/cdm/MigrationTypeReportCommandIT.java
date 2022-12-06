package edu.unc.lib.boxc.migration.cdm;

import edu.unc.lib.boxc.migration.cdm.services.MigrationTypeReportService;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;

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
}
