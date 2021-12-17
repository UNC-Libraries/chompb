/**
 * Copyright 2008 The University of North Carolina at Chapel Hill
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
        Connection conn = indexService.openDbConnection();
        Statement statement = conn.createStatement();
        statement.execute("drop table if exists redirect_mappings");
        statement.execute("CREATE TABLE redirect_mappings (" +
                " id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                " cdm_collection_id varchar(64) NOT NULL, " +
                " cdm_object_id varchar(64) NOT NULL," +
                " boxc_work_id varchar(64) NOT NULL," +
                " boxc_file_id varchar(64) DEFAULT NULL," +
                " created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP," +
                " UNIQUE (cdm_collection_id, cdm_object_id)" +
                ")");
        CdmIndexService.closeDbConnection(conn);

        testHelper.indexExportData("export_1.xml");
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, null);
        testHelper.populateDescriptions("gilmer_mods1.xml");
        testHelper.populateSourceFiles("276_182_E.tif", "276_183B_E.tif", "276_203_E.tif");

        sipsService.generateSips(makeOptions());
    }

    @Test
    public void indexingDoesNotHappenIfCollectionIsNotSubmitted() throws Exception {
        try {
            indexService.indexMapping();
        } catch (InvalidProjectStateException e) {
            assertTrue("Unexpected message: " + e.getMessage(),
                    e.getMessage().contains("Must submit the collection prior to indexing"));
        }
    }

    @Test
    public void redirectMappingTableIsPopulatedWithRightNumberOfRows() throws Exception {
        addSipsSubmitted();
        indexService.indexMapping();

        Connection conn = indexService.openDbConnection();
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select count(*) from redirect_mappings");
            rs.next();
            assertEquals("Incorrect number of rows in database", 3, rs.getInt(1));
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
}
