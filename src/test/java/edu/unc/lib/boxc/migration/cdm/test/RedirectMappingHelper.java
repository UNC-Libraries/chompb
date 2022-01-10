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
package edu.unc.lib.boxc.migration.cdm.test;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.services.CdmIndexService;
import edu.unc.lib.boxc.migration.cdm.services.SipService;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author snluong
 */
public class RedirectMappingHelper {

    public void createRedirectMappingsTableInDb(Path redirectMappingIndexPath) {
        String connectionString = "jdbc:sqlite:" + redirectMappingIndexPath;
        try {
            Class.forName("org.sqlite.JDBC");
            Connection conn = DriverManager.getConnection(connectionString);
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
        } catch (ClassNotFoundException | SQLException e) {
            throw new MigrationException("Failed to open database connection to " + connectionString, e);
        }
    }

    public void setUpCompletedProject(SipServiceHelper testHelper) {
        SipService sipsService = testHelper.createSipsService();
    }
}
