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
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.SipGenerationOptions;
import edu.unc.lib.boxc.migration.cdm.services.CdmIndexService;
import org.apache.commons.io.FileUtils;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author snluong
 */
public class RedirectMappingHelper {
    private static final String USERNAME = "migr_user";
    public static final String REDIRECT_MAPPING_INDEX_FILENAME = "redirect_mapping_index.db";
    private final MigrationProject project;

    public RedirectMappingHelper(MigrationProject project) {
        this.project = project;
    }

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

    /**
     * @return Path of the index containing redirect mapping data
     */
    public Path getRedirectMappingIndexPath() {
        return project.getProjectPath().resolve(REDIRECT_MAPPING_INDEX_FILENAME);
    }

    public SipGenerationOptions makeOptions() {
        SipGenerationOptions options = new SipGenerationOptions();
        options.setUsername(USERNAME);
        return options;
    }

    public Path createRedirectDbConnectionPropertiesFile(TemporaryFolder tempFolder, String dbType) throws IOException {
        String mysqlString = "db_type=mysql\ndb_host=localhost\ndb_user=root\ndb_password=password";
        String sqliteString = "db_type=sqlite\ndb_host=" + getRedirectMappingIndexPath();
        File createdFile = tempFolder.newFile("redirect_db_connection.properties");

        String string = "sqlite".equals(dbType) ? sqliteString : mysqlString;
        FileUtils.writeStringToFile(createdFile, string, StandardCharsets.UTF_8);

        return createdFile.toPath();
    }
}
