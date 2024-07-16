package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.test.BxcEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.RedirectMappingHelper;
import edu.unc.lib.boxc.migration.cdm.test.SipServiceHelper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author snluong
 */
public class RedirectMappingIndexServiceTest {
    private static final String PROJECT_NAME = "proj";
    private static final String DEST_UUID = "7a33f5e6-f0ca-461c-8df0-c76c62198b17";

    @TempDir
    public Path tmpFolder;

    private MigrationProject project;
    private RedirectMappingIndexService indexService;
    private SipService sipsService;
    private SipServiceHelper testHelper;
    private RedirectMappingHelper redirectMappingHelper;

    @BeforeEach
    public void setup() throws Exception {
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder, PROJECT_NAME, null, "user", CdmEnvironmentHelper.DEFAULT_ENV_ID,
                BxcEnvironmentHelper.DEFAULT_ENV_ID, MigrationProject.PROJECT_SOURCE_CDM);
        testHelper = new SipServiceHelper(project, tmpFolder);
        redirectMappingHelper = new RedirectMappingHelper(project);
        redirectMappingHelper.createRedirectMappingsTableInDb();
        Path sqliteDbPropertiesPath = redirectMappingHelper.createDbConnectionPropertiesFile(tmpFolder, "sqlite");

        sipsService = testHelper.createSipsService();
        indexService = new RedirectMappingIndexService(project);
        indexService.setRedirectDbConnectionPath(sqliteDbPropertiesPath);
        indexService.init();
    }

    @Test
    public void indexingDoesNotHappenIfCollectionIsNotSubmitted() {
        try {
            indexService.indexMapping();
            fail();
        } catch (InvalidProjectStateException e) {
            assertTrue(e.getMessage().contains("Must submit the collection prior to indexing"),
                    "Unexpected message: " + e.getMessage());
        }
    }

    @Test
    public void redirectMappingIndexPopulatesTableCorrectly() throws Exception {
        testHelper.initializeDefaultProjectState(DEST_UUID);
        sipsService.generateSips(redirectMappingHelper.makeOptions());

        List<String> row1 = new ArrayList<>();
        Path mappingPath = project.getRedirectMappingPath();
        Connection conn = indexService.openDbConnection();

        testHelper.addSipsSubmitted();
        indexService.indexMapping();

        try (
                Reader reader = Files.newBufferedReader(mappingPath);
                CSVParser originalParser = new CSVParser(reader, CSVFormat.DEFAULT
                        .withFirstRecordAsHeader()
                        .withHeader(RedirectMappingService.CSV_HEADERS)
                        .withTrim());
        ) {
            for (CSVRecord originalRecord : originalParser) {
                row1.add(originalRecord.get(0)); // cdm_collection_id
                row1.add(originalRecord.get(1)); // cdm_object_id
                row1.add(originalRecord.get(2)); // boxc_object_id
                row1.add(originalRecord.get(3)); // boxc_file_id
                break; // just testing the first row
            }

            Statement stmt = conn.createStatement();
            ResultSet count = stmt.executeQuery("select count(*) from redirect_mappings");
            count.next();
            assertEquals(4, count.getInt(1), "Incorrect number of rows in database");

            ResultSet rs = stmt.executeQuery("select cdm_collection_id, cdm_object_id, " +
                    "boxc_object_id, boxc_file_id from redirect_mappings");
            rs.next();
            assertEquals(row1.get(0), rs.getString("cdm_collection_id"),
                    "cdm_collection_id value isn't accurate");
            assertEquals(row1.get(1), rs.getString("cdm_object_id"),
                    "cdm_object_id value isn't accurate");
            assertEquals(row1.get(2), rs.getString("boxc_object_id"),
                    "boxc_object_id value isn't accurate");
            assertEquals(row1.get(3), rs.getString("boxc_file_id"),
                    "boxc_file_id value isn't accurate");
        } finally {
            CdmIndexService.closeDbConnection(conn);
        }
    }

    @Test
    public void tableIsPopulatedCorrectlyInRedirectMappingIndexForCompoundObjects() throws Exception {
        List<String> cdmObjectIds = new ArrayList<>();
        List<String> expectedIds = Arrays.asList("604", "607");
        generateCompoundObjectProject();
        sipsService.generateSips(redirectMappingHelper.makeOptions());
        Connection conn = indexService.openDbConnection();

        testHelper.addSipsSubmitted();
        indexService.indexMapping();

        try {
            Statement stmt = conn.createStatement();
            ResultSet count = stmt.executeQuery("select count(*) from redirect_mappings");
            count.next();
            assertEquals(8, count.getInt(1), "Incorrect number of rows in database");

            ResultSet rs = stmt.executeQuery("select cdm_object_id from redirect_mappings where " +
                    "cdm_object_id is not null and boxc_object_id is not null and boxc_file_id is null");
            while (rs.next()) {
                cdmObjectIds.add(rs.getString("cdm_object_id"));
            }

            assertEquals(expectedIds, cdmObjectIds, "compound objects aren't represented correctly");
        } finally {
            CdmIndexService.closeDbConnection(conn);
        }
    }

    @Test
    public void generateConnectionStringBuildsMySqlString() throws IOException {
        Path mysqlPath = redirectMappingHelper.createDbConnectionPropertiesFile(tmpFolder, "mysql");
        indexService.setRedirectDbConnectionPath(mysqlPath);
        indexService.init();

        assertEquals("jdbc:mysql://root:password@localhost:3306/chomping_block",
                indexService.generateConnectionString(), "generated connection string is incorrect");
    }

    private void generateCompoundObjectProject() throws Exception {
        testHelper.indexExportData(Paths.get("src/test/resources/keepsakes_fields.csv"),
                "mini_keepsakes");
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, null);
        testHelper.getDescriptionsService().generateDocuments(true);
        testHelper.getDescriptionsService().expandDescriptions();
        var options = testHelper.makeSourceFileOptions(testHelper.getSourceFilesBasePath());
        options.setExportField("filena");
        testHelper.populateSourceFiles(options, "nccg_ck_09.tif", "nccg_ck_1042-22_v1.tif",
                "nccg_ck_1042-22_v2.tif", "nccg_ck_549-4_v1.tif", "nccg_ck_549-4_v2.tif");
    }
}
