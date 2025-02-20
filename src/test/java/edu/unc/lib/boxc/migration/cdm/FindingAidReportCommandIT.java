package edu.unc.lib.boxc.migration.cdm;

import edu.unc.lib.boxc.migration.cdm.services.CdmFieldService;
import edu.unc.lib.boxc.migration.cdm.services.FindingAidService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

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

    @Test
    public void collectionReportTest() throws Exception {
        Files.copy(Paths.get("src/test/resources/roy_brown/cdm_fields.csv"), project.getFieldsPath());
        CdmFieldService fieldService = new CdmFieldService();
        FindingAidService findingAidService = new FindingAidService();
        findingAidService.setCdmFieldService(fieldService);
        findingAidService.setProject(project);
        findingAidService.recordFindingAid();
        testHelper.indexExportData("03883");

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "finding_aid_reports", "collection_report"};
        executeExpectSuccess(args);

        assertOutputMatches(".*Hook id: +contri+.*");
        assertOutputMatches(".*Collection number: +descri+.*");
        assertOutputMatches(".*03883.*");
        assertOutputMatches(".*Records with collection ids: +3 \\(100.0%\\).*");
        assertOutputMatches(".*Records with hook ids: +3 \\(100.0%\\).*");
        assertOutputMatches(".*collec:.*3\\/3.*100%.*");
        assertOutputMatches(".*descri:.*3\\/3.*100%.*");
        assertOutputMatches(".*findin:.*3\\/3.*100%.*");
        assertOutputMatches(".*locati:.*3\\/3.*100%.*");
        assertOutputMatches(".*title:.*3\\/3.*100%.*");
        assertOutputMatches(".*prefer:.*3\\/3.*100%.*");
        assertOutputMatches(".*creato:.*3\\/3.*100%.*");
        assertOutputMatches(".*contri:.*3\\/3.*100%.*");
        assertOutputMatches(".*relatid:.*3\\/3.*100%.*");
    }
}
