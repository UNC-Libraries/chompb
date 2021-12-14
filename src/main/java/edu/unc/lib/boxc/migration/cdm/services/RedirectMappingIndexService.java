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
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Service for indexing the redirect mapping CSV into the chomping_block DB
 *
 * @author snluong
 */
public class RedirectMappingIndexService {
    private MigrationProject project;

    public RedirectMappingIndexService(MigrationProject project) {
        this.project = project;
    }

    public void indexMapping() {
        assertCollectionSubmitted();
        Connection conn = null;
        try {
            conn = openDbConnection();
            // execute SQL statement to index csv
            // read rows from redirect mapping csv and insert into table
        } catch (SQLException e) {
            throw new MigrationException("Error indexing redirect mapping CSV", e);
        } finally {
            closeDbConnection(conn);
        }
    }

    private void assertCollectionSubmitted() {
        if (project.getProjectProperties().getSipsSubmitted() == null) {
            throw new InvalidProjectStateException("Must submit the collection prior to indexing");
        }
    }

    public Connection openDbConnection() throws SQLException {
        try {
//            Class.forName("org.sqlite.JDBC");
//            return DriverManager.getConnection("jdbc:sqlite:" + project.getIndexPath());
        } catch (ClassNotFoundException e) {
            throw new MigrationException("Failed to open database connection to " + project.getIndexPath(), e);
            // what is the index path?
        }
    }

    public static void closeDbConnection(Connection conn) {
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            throw new MigrationException("Failed to close database connection: " + e.getMessage());
        }
    }
}
