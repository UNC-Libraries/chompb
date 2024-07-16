package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.test.BxcEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.SipServiceHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author krwong
 */
public class MigrationTypeReportServiceTest {
    private static final String BOXC_BASE_URL = "http://localhost:46887/bxc/record/";
    private static final String BOXC_ID_1 = "bb3b83d7-2962-4604-a7d0-9afcb4ec99b1";
    private static final String BOXC_ID_2 = "91c08272-260f-40f1-bb7c-78854d504368";
    private static final String BOXC_ID_3 = "f9d7262c-3cfb-4d27-8ecc-b9df9ac2f950";
    private static final String BOXC_URL_1 = BOXC_BASE_URL + BOXC_ID_1;
    private static final String BOXC_URL_2 = BOXC_BASE_URL + BOXC_ID_2;
    private static final String BOXC_URL_3 = BOXC_BASE_URL + BOXC_ID_3;
    private static final String CDM_URL_1 = "http://localhost/cdm/singleitem/collection/proj/id/25";
    private static final String CDM_URL_2 = "http://localhost/cdm/singleitem/collection/proj/id/26";
    private static final String CDM_URL_3 = "http://localhost/cdm/singleitem/collection/proj/id/27";
    @TempDir
    public Path tmpFolder;

    private SipServiceHelper testHelper;
    private MigrationProject project;
    private PostMigrationReportService reportGenerator;
    private MigrationTypeReportService service;

    @BeforeEach
    public void setup() throws Exception {
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder, "proj", null, "user", CdmEnvironmentHelper.DEFAULT_ENV_ID,
                BxcEnvironmentHelper.DEFAULT_ENV_ID, MigrationProject.PROJECT_SOURCE_CDM);
        testHelper = new SipServiceHelper(project, tmpFolder);
        reportGenerator = new PostMigrationReportService();
        reportGenerator.setProject(project);
        reportGenerator.setChompbConfig(testHelper.getChompbConfig());
        service = new MigrationTypeReportService();
        service.setProject(project);
    }

    @Test
    public void reportCountWorksTest() throws Exception {
        reportGenerator.init();
        reportGenerator.addRow("25", CDM_URL_1, "Work", BOXC_URL_1, "Redoubt C",
                null, null, null, "", "", 1);
        reportGenerator.addRow("26", CDM_URL_2, "File", BOXC_URL_2, "A file",
                null, null, null, BOXC_URL_1, "Redoubt C", null);
        reportGenerator.closeCsv();

        long numWorks = service.countWorks();
        assertEquals(1, numWorks);
    }

    @Test
    public void reportCountFilesTest() throws Exception {
        reportGenerator.init();
        reportGenerator.addRow("25", CDM_URL_1, "Work", BOXC_URL_1, "Redoubt C",
                null, null, null, "", "", 1);
        reportGenerator.addRow("26", CDM_URL_2, "File", BOXC_URL_2, "A file",
                null, null, null, BOXC_URL_1, "Redoubt C", null);
        reportGenerator.addRow("27", CDM_URL_3, "File", BOXC_URL_3, "A file",
                null, null, null, BOXC_URL_1, "Redoubt C", null);
        reportGenerator.closeCsv();

        long numFiles = service.countFiles();
        assertEquals(2, numFiles);
    }

}
