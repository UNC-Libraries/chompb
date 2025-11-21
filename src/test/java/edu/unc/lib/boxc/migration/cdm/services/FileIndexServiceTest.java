package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.ExportObjectsInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.CdmIndexOptions;
import edu.unc.lib.boxc.migration.cdm.test.BxcEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import static edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo.CDM_ID;
import static edu.unc.lib.boxc.migration.cdm.services.CdmFieldService.CSV;
import static edu.unc.lib.boxc.migration.cdm.services.CdmFieldService.EAD_TO_CDM;
import static edu.unc.lib.boxc.migration.cdm.test.IndexServiceHelper.assertDateIndexedPresent;
import static edu.unc.lib.boxc.migration.cdm.test.IndexServiceHelper.mappingBody;
import static edu.unc.lib.boxc.migration.cdm.test.IndexServiceHelper.setExportedDate;
import static edu.unc.lib.boxc.migration.cdm.test.IndexServiceHelper.writeCsv;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmUtil.COLLECTION_NAME;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmUtil.REF_ID;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmUtil.standardizeHeader;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class FileIndexServiceTest {

    private static final String PROJECT_NAME = "proj";
    @TempDir
    public Path tmpFolder;

    private MigrationProject project;
    private CdmFieldService fieldService;
    private FileIndexService service;

    @BeforeEach
    public void setup() throws Exception {
        project = MigrationProjectFactory.createCdmMigrationProject(
                tmpFolder, PROJECT_NAME, null, "user",
                CdmEnvironmentHelper.DEFAULT_ENV_ID, BxcEnvironmentHelper.DEFAULT_ENV_ID);
        Files.createDirectories(project.getExportPath());

        fieldService = new CdmFieldService();
        service = new FileIndexService();
        service.setFieldService(fieldService);
        service.setProject(project);
    }

    @Test
    public void indexFromCsvTest() throws Exception {
        CdmFieldInfo csvExportFields = fieldService.retrieveFields(Paths.get("src/test/resources/files/exported_objects.csv"), CSV);
        fieldService.persistFieldsToProject(project, csvExportFields);
        setExportedDate(project);
        CdmIndexOptions options = new CdmIndexOptions();
        options.setCsvFile(Paths.get("src/test/resources/files/exported_objects.csv"));
        options.setForce(false);

        service.createDatabase(options);
        service.indexAllFromFile(options);

        assertDateIndexedPresent(project);
        assertRowCount(3);

        CdmFieldInfo fieldInfo = fieldService.loadFieldsFromProject(project);
        List<String> exportFields = fieldInfo.listAllExportFields();

        Connection conn = service.openDbConnection();
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select " + String.join(",", exportFields)
                    + " from " + CdmIndexService.TB_NAME + " order by " + ExportObjectsInfo.RECORD_ID + " asc");
            rs.next();
            assertEquals("test-00001", rs.getString(ExportObjectsInfo.RECORD_ID));
            assertEquals("src/test/resources/files/D2_035_Varners_DrugStore_interior.tif",
                    rs.getString(ExportObjectsInfo.FILE_PATH));
            assertEquals("D2_035_Varners_DrugStore_interior.tif", rs.getString(ExportObjectsInfo.FILENAME));

            rs.next();
            assertEquals("test-00002", rs.getString(ExportObjectsInfo.RECORD_ID));
            assertEquals("src/test/resources/files/MJM_7_016_LumberMills_IndianCreekTrestle.tif",
                    rs.getString(ExportObjectsInfo.FILE_PATH));
            assertEquals("MJM_7_016_LumberMills_IndianCreekTrestle.tif", rs.getString(ExportObjectsInfo.FILENAME));

            rs.next();
            assertEquals("test-00003", rs.getString(ExportObjectsInfo.RECORD_ID));
            assertEquals("src/test/resources/files/IMG_2377.jpeg", rs.getString(ExportObjectsInfo.FILE_PATH));
            assertEquals("IMG_2377.jpeg", rs.getString(ExportObjectsInfo.FILENAME));
        } finally {
            CdmIndexService.closeDbConnection(conn);
        }
    }

    @Test
    public void indexFromCsvMoreFieldsTest() throws Exception {
        CdmFieldInfo csvExportFields = fieldService.retrieveFields(Paths.get("src/test/resources/files/more_fields.csv"), CSV);
        fieldService.persistFieldsToProject(project, csvExportFields);
        setExportedDate(project);
        CdmIndexOptions options = new CdmIndexOptions();
        options.setCsvFile(Paths.get("src/test/resources/files/more_fields.csv"));
        options.setForce(false);

        service.createDatabase(options);
        service.indexAllFromFile(options);

        assertDateIndexedPresent(project);
        assertRowCount(3);

        CdmFieldInfo fieldInfo = fieldService.loadFieldsFromProject(project);
        List<String> exportFields = fieldInfo.listAllExportFields();

        Connection conn = service.openDbConnection();
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select " + String.join(",", exportFields)
                    + " from " + CdmIndexService.TB_NAME + " order by " + ExportObjectsInfo.RECORD_ID + " asc");
            rs.next();
            assertEquals("test-00001", rs.getString(ExportObjectsInfo.RECORD_ID));
            assertEquals("src/test/resources/files/D2_035_Varners_DrugStore_interior.tif",
                    rs.getString(ExportObjectsInfo.FILE_PATH));
            assertEquals("D2_035_Varners_DrugStore_interior.tif", rs.getString(ExportObjectsInfo.FILENAME));
            assertEquals("tif", rs.getString("file_type"));

            rs.next();
            assertEquals("test-00002", rs.getString(ExportObjectsInfo.RECORD_ID));
            assertEquals("src/test/resources/files/MJM_7_016_LumberMills_IndianCreekTrestle.tif",
                    rs.getString(ExportObjectsInfo.FILE_PATH));
            assertEquals("MJM_7_016_LumberMills_IndianCreekTrestle.tif", rs.getString(ExportObjectsInfo.FILENAME));
            assertEquals("tif", rs.getString("file_type"));

            rs.next();
            assertEquals("test-00003", rs.getString(ExportObjectsInfo.RECORD_ID));
            assertEquals("src/test/resources/files/IMG_2377.jpeg", rs.getString(ExportObjectsInfo.FILE_PATH));
            assertEquals("IMG_2377.jpeg", rs.getString(ExportObjectsInfo.FILENAME));
            assertEquals("jpeg", rs.getString("file_type"));
        } finally {
            CdmIndexService.closeDbConnection(conn);
        }
    }

    @Test
    public void indexFromEadToCdmTsvTest() throws Exception {
        // make source_files.csv
        writeCsv(project, mappingBody("00001,," + project.getProjectPath() + "/00011_0045_0001.tif,",
                "00002,," + project.getProjectPath() + "/00011_0045_0002.tif,"));
        var path = Paths.get("src/test/resources/files/ead_to_cdm.tsv");
        var formattedEadToCdmTsvPath = service.addIdsToEadToCdmTsv(path);

        // what is sent to the index service should be the formatted, standardized TSV
        CdmFieldInfo fileExportFields = fieldService.retrieveFields(formattedEadToCdmTsvPath, EAD_TO_CDM);
        fieldService.persistFieldsToProject(project, fileExportFields);
        setExportedDate(project);
        CdmIndexOptions options = new CdmIndexOptions();
        options.setEadTsvFile(path);
        options.setForce(false);

        service.setSource(EAD_TO_CDM);
        service.createDatabase(options);
        service.indexAllFromFile(options);

        assertDateIndexedPresent(project);
        assertRowCount(2);

        CdmFieldInfo fieldInfo = fieldService.loadFieldsFromProject(project);
        List<String> exportFields = fieldInfo.listAllExportFields();

        Connection conn = service.openDbConnection();
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select " + String.join(",", exportFields)
                    + " from " + IndexService.TB_NAME + " order by " + CDM_ID + " asc");
            rs.next();
            assertEquals("00001", rs.getString(CDM_ID));
            assertEquals("Alexander and Hillhouse Family Papers, 11", rs.getString(standardizeHeader(COLLECTION_NAME)));
            assertEquals("8fb42a5daf43b6711ccf38b1971a671d", rs.getString(standardizeHeader(REF_ID)));

            rs.next();
            assertEquals("00002", rs.getString(CDM_ID));
            assertEquals("Alexander and Hillhouse Family Papers, 11", rs.getString(standardizeHeader(COLLECTION_NAME)));
            assertEquals("8fb42a5daf43b6711ccf38b1971a671d", rs.getString(standardizeHeader(REF_ID)));

        } finally {
            CdmIndexService.closeDbConnection(conn);
        }
    }

    @Test
    public void indexFromFileNoFileExists() {
        assertThrows(InvalidProjectStateException.class, () -> {
            var path = Paths.get("src/test/resources/files/no_file.tsv");
            CdmIndexOptions options = new CdmIndexOptions();
            options.setEadTsvFile(path);
            options.setForce(false);

            service.setSource(EAD_TO_CDM);
            service.indexAllFromFile(options);
        });
    }

    public void assertRowCount(int expected) throws Exception {
        Connection conn = service.openDbConnection();
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select count(*) from " + CdmIndexService.TB_NAME);
            rs.next();
            assertEquals(expected, rs.getInt(1), "Incorrect number of rows in database");
        } finally {
            IndexService.closeDbConnection(conn);
        }
    }
}
