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
import edu.unc.lib.boxc.migration.cdm.options.SipGenerationOptions;
import edu.unc.lib.boxc.migration.cdm.test.SipServiceHelper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

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
    private static final String USERNAME = "migr_user";
    private static final String DEST_UUID = "7a33f5e6-f0ca-461c-8df0-c76c62198b17";

    @Rule
    public final TemporaryFolder tmpFolder = new TemporaryFolder();

    private MigrationProject project;
    private RedirectMappingIndexService indexService;
    private SipService sipsService;
    private SipServiceHelper testHelper;

    @Before
    public void setup() throws Exception {
        tmpFolder.create();
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder.getRoot().toPath(), PROJECT_NAME, null, "user");
        testHelper = new SipServiceHelper(project, tmpFolder.newFolder().toPath());
        sipsService = testHelper.createSipsService();
        indexService = new RedirectMappingIndexService(project);
        // the tests use a sqlite DB but otherwise the service will connect to a MySQL DB
        indexService.setConnectionString("jdbc:sqlite:" + testHelper.getRedirectMappingIndexPath());

        Connection conn = indexService.openDbConnection();
        Statement statement = conn.createStatement();
        statement.execute("drop table if exists redirect_mappings");
        statement.execute("CREATE TABLE redirect_mappings (" +
                "id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                "cdm_collection_id varchar(64) NOT NULL, " +
                "cdm_object_id varchar(64) NOT NULL, " +
                "boxc_work_id varchar(64) NOT NULL, " +
                "boxc_file_id varchar(64) DEFAULT NULL, " +
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, " +
                "UNIQUE (cdm_collection_id, cdm_object_id)" +
                ")");
        CdmIndexService.closeDbConnection(conn);
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
        generateRegularProject();
        sipsService.generateSips(makeOptions());

        List<String> row1 = new ArrayList<>();
        Path mappingPath = project.getRedirectMappingPath();
        Connection conn = indexService.openDbConnection();

        addSipsSubmitted();
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
                row1.add(originalRecord.get(2)); // boxc_work_id
                row1.add(originalRecord.get(3)); // boxc_file_id
                break; // just testing the first row
            }

            Statement stmt = conn.createStatement();
            ResultSet count = stmt.executeQuery("select count(*) from redirect_mappings");
            count.next();
            assertEquals("Incorrect number of rows in database", 3, count.getInt(1));

            ResultSet rs = stmt.executeQuery("select cdm_collection_id, cdm_object_id, " +
                    "boxc_work_id, boxc_file_id from redirect_mappings");
            rs.next();
            assertEquals("cdm_collection_id value isn't accurate", row1.get(0),
                    rs.getString("cdm_collection_id"));
            assertEquals("cdm_object_id value isn't accurate", row1.get(1),
                    rs.getString("cdm_object_id"));
            assertEquals("boxc_work_id value isn't accurate", row1.get(2),
                    rs.getString("boxc_work_id"));
            assertEquals("boxc_file_id value isn't accurate", row1.get(3),
                    rs.getString("boxc_file_id"));
        } finally {
            CdmIndexService.closeDbConnection(conn);
        }
    }

    @Test
    public void tableIsPopulatedCorrectlyInRedirectMappingIndexForCompoundObjects() throws Exception {
        List<String> cdm_object_ids = new ArrayList<>();
        List<String> expected_ids = Arrays.asList("604", "607");
        generateCompoundObjectProject();
        sipsService.generateSips(makeOptions());
        Connection conn = indexService.openDbConnection();

        addSipsSubmitted();
        indexService.indexMapping();

        try {
            Statement stmt = conn.createStatement();
            ResultSet count = stmt.executeQuery("select count(*) from redirect_mappings");
            count.next();
            assertEquals("Incorrect number of rows in database", 7, count.getInt(1));

            ResultSet rs = stmt.executeQuery("select cdm_object_id from redirect_mappings where " +
                    "boxc_work_id is not null and boxc_file_id is null");
            while (rs.next()) {
                cdm_object_ids.add(rs.getString("cdm_object_id"));
            }

            assertEquals("compound objects aren't represented correctly", expected_ids, cdm_object_ids);
        } finally {
            CdmIndexService.closeDbConnection(conn);
        }
    }

    private SipGenerationOptions makeOptions() {
        SipGenerationOptions options = new SipGenerationOptions();
        options.setUsername(USERNAME);
        return options;
    }

    private void addSipsSubmitted() {
        project.getProjectProperties().getSipsSubmitted().add("Sips submitted!");
    }

    private void generateRegularProject() throws Exception {
        testHelper.indexExportData("export_1.xml");
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, null);
        testHelper.populateDescriptions("gilmer_mods1.xml");
        testHelper.populateSourceFiles("276_182_E.tif", "276_183B_E.tif", "276_203_E.tif");
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
