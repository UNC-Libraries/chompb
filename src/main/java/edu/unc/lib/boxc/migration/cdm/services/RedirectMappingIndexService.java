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
import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

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
        Path mappingPath = project.getRedirectMappingPath();
        try {
            conn = openDbConnection();
            Reader reader = Files.newBufferedReader(mappingPath);
            CSVParser originalParser = new CSVParser(reader, CSVFormat.DEFAULT
                    .withFirstRecordAsHeader()
                    .withHeader(RedirectMappingService.CSV_HEADERS)
                    .withTrim());

            for (CSVRecord originalRecord : originalParser) {
                String cdm_collection_id = originalRecord.get(0);
                String cdm_object_id = originalRecord.get(1);
                String boxc_work_id = originalRecord.get(2);
                String boxc_file_id = originalRecord.get(3);

                String query = " insert into redirect_mappings (cdm_collection_id, cdm_object_id, boxc_work_id, boxc_file_id)"
                        + " values (?, ?, ?, ?)";
                PreparedStatement preparedStatement = conn.prepareStatement(query);
                preparedStatement.setString(1, cdm_collection_id);
                preparedStatement.setString(2, cdm_object_id);
                preparedStatement.setString(3, boxc_work_id);
                if (boxc_file_id.isEmpty()) {
                    preparedStatement.setNull(4, java.sql.Types.NULL);
                } else {
                    preparedStatement.setString(4, boxc_file_id);
                }

                preparedStatement.execute();
            }

            // insert or update (if it's the same IDs?) if it fails, delete the old one and put it in again?
        } catch (SQLException | IOException e) {
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
