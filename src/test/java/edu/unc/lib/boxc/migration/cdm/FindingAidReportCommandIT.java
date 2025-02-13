package edu.unc.lib.boxc.migration.cdm;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class FindingAidReportCommandIT extends AbstractCommandIT {
    private Path basePath;

    @BeforeEach
    public void setup() throws Exception {
        initProjectAndHelper();
        basePath = tmpFolder;
    }

    @Test
    public void generateNotIndexedTest() throws Exception {
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "finding_aid_reports", "hookid_report"};
        executeExpectFailure(args);

        assertOutputContains("Project must be indexed");
    }

    @Test
    public void descriFieldReportTest() throws Exception {
        testHelper.indexExportData("03883");
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "finding_aid_reports", "field_report",
                "-f", "descri"};
        executeExpectSuccess(args);

        assertTrue(Files.exists(project.getProjectPath().resolve("descri_report.csv")));
    }

    @Test
    public void hookIdReportTest() throws Exception {
        testHelper.indexExportData("03883");
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "finding_aid_reports", "hookid_report"};
        executeExpectSuccess(args);

        assertTrue(Files.exists(project.getProjectPath().resolve("hookid_report.csv")));
    }
}
