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
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.DriverManager;
import java.util.Properties;

/**
 * Service for indexing the redirect mapping CSV into the chomping_block DB
 *
 * @author snluong
 */
public class RedirectMappingIndexService {
    private final MigrationProject project;
    public String connectionString;
    public static final String INSERT_STATEMENT = " insert into redirect_mappings " +
            "(cdm_collection_id, cdm_object_id, boxc_object_id, boxc_file_id) " +
            "values (?, ?, ?, ?)";
    private Path redirectDbConnectionPath;
    private Properties props;

    public RedirectMappingIndexService(MigrationProject project) {
        this.project = project;
    }

    public void init() {
        try (InputStream inputStream = Files.newInputStream(redirectDbConnectionPath)) {
            props = new Properties();
            props.load(inputStream);
        } catch (IOException e) {
            throw new MigrationException("Error reading properties file", e);
        }
    }

    /**
     * Reads data from redirect_mapping CSV and inserts that data into the redirect_mappings table
     */
    public void indexMapping() {
        assertCollectionSubmitted();
        Connection conn = null;
        Path mappingPath = project.getRedirectMappingPath();
        try (
            Reader reader = Files.newBufferedReader(mappingPath);
            CSVParser originalParser = new CSVParser(reader, CSVFormat.DEFAULT
                    .withFirstRecordAsHeader()
                    .withHeader(RedirectMappingService.CSV_HEADERS)
                    .withTrim());
        ) {
            conn = openDbConnection();
            for (CSVRecord originalRecord : originalParser) {
                String cdmCollectionId = originalRecord.get(0);
                String cdmObjectId = originalRecord.get(1);
                String boxcObjectId = originalRecord.get(2);
                String boxcFileId = originalRecord.get(3);

                PreparedStatement preparedStatement = conn.prepareStatement(INSERT_STATEMENT);
                preparedStatement.setString(1, cdmCollectionId);
                if (cdmObjectId.isEmpty()) {
                    preparedStatement.setNull(2, java.sql.Types.NULL);
                } else {
                    preparedStatement.setString(2, cdmObjectId);
                }
                preparedStatement.setString(3, boxcObjectId);
                if (boxcFileId.isEmpty()) {
                    preparedStatement.setNull(4, java.sql.Types.NULL);
                } else {
                    preparedStatement.setString(4, boxcFileId);
                }

                preparedStatement.execute();
            }

            // TODO insert or update (if it's the same IDs?) if it fails, delete the old one and put it in again?
        } catch (SQLException | IOException e) {
            throw new MigrationException("Error indexing redirect mapping CSV", e);
        } finally {
            CdmIndexService.closeDbConnection(conn);
        }
    }

    private void assertCollectionSubmitted() {
        if (project.getProjectProperties().getSipsSubmitted().isEmpty()) {
            throw new InvalidProjectStateException("Must submit the collection prior to indexing");
        }
    }

    public Connection openDbConnection() throws SQLException {
        try {
            // in the tests we use sqlite, otherwise mysql is used
            if (connectionString == null) {
                setConnectionString(generateConnectionString());
            }
            if (connectionString.contains("sqlite")) {
                Class.forName("org.sqlite.JDBC");
            } else {
                Class.forName("com.mysql.cj.jdbc.Driver");
            }

            return DriverManager.getConnection(connectionString);
        } catch (ClassNotFoundException e) {
            throw new MigrationException("Failed to open database connection to " + connectionString, e);
        }
    }

    public void setConnectionString(String connectionString) {
        this.connectionString = connectionString;
    }

    public void setRedirectDbConnectionPath(Path redirectDbConnectionPath) {
        this.redirectDbConnectionPath = redirectDbConnectionPath;
    }

    /*
        Determines DB type from loaded properties and builds a connection string accordingly
        This connection string is used to connect to the DB where the redirect mapping table lives
     */
    public String generateConnectionString() {
        String dbType = props.getProperty("db_type");
        String host = props.getProperty("db_host");

        // we use sqlite in the tests, which use a different connection string syntax
        if ("sqlite".equals(dbType) ) {
            return "jdbc:sqlite:" + host;
        }

        String user = props.getProperty("db_user");
        String password = props.getProperty("db_password");

        return "jdbc:" + dbType + "://" + user + ":" + password + "@" + host + ":3306/chomping_block";
    }
}
