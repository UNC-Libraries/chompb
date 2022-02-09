/**
 * Copyright 2008 The University of North Carolina at Chapel Hill
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.test.RedirectMappingHelper;
import edu.unc.lib.boxc.migration.cdm.test.SipServiceHelper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author snluong
 */
public class RedirectMappingIndexServiceTest {
    private static final String PROJECT_NAME = "proj";
    private static final String DEST_UUID = "7a33f5e6-f0ca-461c-8df0-c76c62198b17";

    @Rule
    public final TemporaryFolder tmpFolder = new TemporaryFolder();

    private MigrationProject project;
    private RedirectMappingIndexService indexService;
    private SipService sipsService;
    private SipServiceHelper testHelper;
    private RedirectMappingHelper redirectMappingHelper;

    @Before
    public void setup() throws Exception {
        tmpFolder.create();
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder.getRoot().toPath(), PROJECT_NAME, null, "user");
        testHelper = new SipServiceHelper(project, tmpFolder.newFolder().toPath());
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
            assertTrue("Unexpected message: " + e.getMessage(),
                    e.getMessage().contains("Must submit the collection prior to indexing"));
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
            assertEquals("Incorrect number of rows in database", 4, count.getInt(1));

            ResultSet rs = stmt.executeQuery("select cdm_collection_id, cdm_object_id, " +
                    "boxc_object_id, boxc_file_id from redirect_mappings");
            rs.next();
            assertEquals("cdm_collection_id value isn't accurate", row1.get(0),
                    rs.getString("cdm_collection_id"));
            assertEquals("cdm_object_id value isn't accurate", row1.get(1),
                    rs.getString("cdm_object_id"));
            assertEquals("boxc_object_id value isn't accurate", row1.get(2),
                    rs.getString("boxc_object_id"));
            assertEquals("boxc_file_id value isn't accurate", row1.get(3),
                    rs.getString("boxc_file_id"));
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
            assertEquals("Incorrect number of rows in database", 8, count.getInt(1));

            ResultSet rs = stmt.executeQuery("select cdm_object_id from redirect_mappings where " +
                    "cdm_object_id is not null and boxc_object_id is not null and boxc_file_id is null");
            while (rs.next()) {
                cdmObjectIds.add(rs.getString("cdm_object_id"));
            }

            assertEquals("compound objects aren't represented correctly", expectedIds, cdmObjectIds);
        } finally {
            CdmIndexService.closeDbConnection(conn);
        }
    }

    @Test
    public void generateConnectionStringBuildsMySqlString() throws IOException {
        Path mysqlPath = redirectMappingHelper.createDbConnectionPropertiesFile(tmpFolder, "mysql");
        indexService.setRedirectDbConnectionPath(mysqlPath);
        indexService.init();

        assertEquals("generated connection string is incorrect",
                "jdbc:mysql://root:password@localhost:3306/chomping_block",
                indexService.generateConnectionString());
    }

    private void generateCompoundObjectProject() throws Exception {
        testHelper.indexExportData(Paths.get("src/test/resources/keepsakes_fields.csv"),
                "export_compounds.xml");
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, null);
        testHelper.getDescriptionsService().generateDocuments(true);
        testHelper.getDescriptionsService().expandDescriptions();
        testHelper.populateSourceFiles("nccg_ck_09.tif", "nccg_ck_1042-22_v1.tif",
                "nccg_ck_1042-22_v2.tif", "nccg_ck_549-4_v1.tif", "nccg_ck_549-4_v2.tif");
    }
}
