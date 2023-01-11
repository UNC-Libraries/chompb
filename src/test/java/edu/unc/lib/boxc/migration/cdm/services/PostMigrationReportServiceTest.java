package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.test.BxcEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.SipServiceHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.nio.file.Paths;

import static edu.unc.lib.boxc.migration.cdm.test.PostMigrationReportTestHelper.assertContainsRow;
import static edu.unc.lib.boxc.migration.cdm.test.PostMigrationReportTestHelper.parseReport;
import static org.mockito.MockitoAnnotations.initMocks;

/**
 * @author bbpennel
 */
public class PostMigrationReportServiceTest {
    private static final String BOXC_BASE_URL = "http://localhost:46887/bxc/record/";
    private static final String BOXC_ID_1 = "bb3b83d7-2962-4604-a7d0-9afcb4ec99b1";
    private static final String BOXC_ID_2 = "91c08272-260f-40f1-bb7c-78854d504368";
    private static final String BOXC_ID_3 = "f9d7262c-3cfb-4d27-8ecc-b9df9ac2f950";
    private static final String BOXC_URL_1 = BOXC_BASE_URL + BOXC_ID_1;
    private static final String BOXC_URL_2 = BOXC_BASE_URL + BOXC_ID_2;
    private static final String BOXC_URL_3 = BOXC_BASE_URL + BOXC_ID_3;
    @TempDir
    public Path tmpFolder;

    private MigrationProject project;
    private SipServiceHelper testHelper;
    private DescriptionsService descriptionsService;
    private PostMigrationReportService service;

    @BeforeEach
    public void setup() throws Exception {
        initMocks(this);
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder, "proj", null, "user",
                CdmEnvironmentHelper.DEFAULT_ENV_ID, BxcEnvironmentHelper.DEFAULT_ENV_ID);
        testHelper = new SipServiceHelper(project, tmpFolder);
        descriptionsService = testHelper.getDescriptionsService();

        service = new PostMigrationReportService();
        service.setProject(project);
        service.setChompbConfig(testHelper.getChompbConfig());
        service.setDescriptionsService(testHelper.getDescriptionsService());
        service.init();
    }

    @Test
    public void addSingleItemTest() throws Exception {
        testHelper.populateDescriptions("gilmer_mods1.xml");

        service.addWorkRow("25", BOXC_ID_1, 1, true);
        service.addFileRow("25/original_file", "25", BOXC_ID_1, BOXC_ID_2, true);
        service.closeCsv();

        var rows = parseReport(project);
        assertContainsRow(rows, "25",
                "http://localhost/cdm/singleitem/collection/proj/id/25",
                "Work",
                BOXC_URL_1,
                "Redoubt C",
                "",
                "",
                "",
                "1");
        assertContainsRow(rows, "25/original_file",
                "http://localhost/cdm/singleitem/collection/proj/id/25",
                "File",
                BOXC_URL_2,
                "",
                "",
                BOXC_URL_1,
                "Redoubt C",
                "");
    }

    @Test
    public void addSingleItemWithFileDescTest() throws Exception {
        testHelper.populateDescriptions("gilmer_mods1.xml", "gilmer_mods_children.xml");

        service.addWorkRow("25", BOXC_ID_1, 1, true);
        service.addFileRow("25/original_file", "25", BOXC_ID_1, BOXC_ID_2, true);
        service.closeCsv();

        var rows = parseReport(project);
        assertContainsRow(rows, "25",
                "http://localhost/cdm/singleitem/collection/proj/id/25",
                "Work",
                BOXC_URL_1,
                "Redoubt C",
                "",
                "",
                "",
                "1");
        assertContainsRow(rows, "25/original_file",
                "http://localhost/cdm/singleitem/collection/proj/id/25",
                "File",
                BOXC_URL_2,
                "Redoubt C Scan File",
                "",
                BOXC_URL_1,
                "Redoubt C",
                "");
    }

    @Test
    public void addGroupedTest() throws Exception {
        testHelper.populateDescriptions("grouped_mods.xml");

        service.addWorkRow("grp:groupa:group1", BOXC_ID_1, 2, false);
        service.addFileRow("26", "grp:groupa:group1", BOXC_ID_1, BOXC_ID_2, false);
        service.addFileRow("27", "grp:groupa:group1", BOXC_ID_1, BOXC_ID_3, false);
        service.closeCsv();

        var rows = parseReport(project);
        assertContainsRow(rows, "grp:groupa:group1",
                "",
                "Work",
                BOXC_URL_1,
                "Folder Group 1",
                "",
                "",
                "",
                "2");
        assertContainsRow(rows, "26",
                "http://localhost/cdm/singleitem/collection/proj/id/26",
                "File",
                BOXC_URL_2,
                "Plan of Battery McIntosh",
                "",
                BOXC_URL_1,
                "Folder Group 1",
                "");
        assertContainsRow(rows, "27",
                "http://localhost/cdm/singleitem/collection/proj/id/27",
                "File",
                BOXC_URL_3,
                "Fort DeRussy on Red River, Louisiana",
                "",
                BOXC_URL_1,
                "Folder Group 1",
                "");
    }

    @Test
    public void addCompoundTest() throws Exception {
        testHelper.indexExportData(Paths.get("src/test/resources/keepsakes_fields.csv"), "mini_keepsakes");
        descriptionsService.generateDocuments(true);
        descriptionsService.expandDescriptions();

        service.addWorkRow("605", BOXC_ID_1, 2, false);
        service.addFileRow("602", "605", BOXC_ID_1, BOXC_ID_2, false);
        service.addFileRow("603", "605", BOXC_ID_1, BOXC_ID_3, false);
        service.closeCsv();

        var rows = parseReport(project);
        assertContainsRow(rows, "605",
                "http://localhost/cdm/compoundobject/collection/proj/id/605",
                "Work",
                BOXC_URL_1,
                "Tiffany's pillbox commemorating UNC's bicentennial (closed, in box)",
                "",
                "",
                "",
                "2");
        assertContainsRow(rows, "602",
                "http://localhost/cdm/singleitem/collection/proj/id/602",
                "File",
                BOXC_URL_2,
                "World War II ration book",
                "",
                BOXC_URL_1,
                "Tiffany's pillbox commemorating UNC's bicentennial (closed, in box)",
                "");
        assertContainsRow(rows, "603",
                "http://localhost/cdm/singleitem/collection/proj/id/603",
                "File",
                BOXC_URL_3,
                "World War II ration book (instructions)",
                "",
                BOXC_URL_1,
                "Tiffany's pillbox commemorating UNC's bicentennial (closed, in box)",
                "");
    }
}
