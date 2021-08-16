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

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.slf4j.Logger;

import edu.unc.lib.boxc.common.xml.SecureXMLFactory;
import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.exceptions.StateAlreadyExistsException;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;

/**
 * Service for populating and querying the index of exported CDM records for a migration project
 * @author bbpennel
 */
public class CdmIndexService {
    private static final Logger log = getLogger(CdmIndexService.class);
    public static final String TB_NAME = "cdm_records";

    private MigrationProject project;
    private CdmFieldService fieldService;

    /**
     * Indexes all exported CDM records for this project
     * @throws IOException
     */
    public void indexAll() throws IOException {
        assertCollectionExported();

        CdmFieldInfo fieldInfo = fieldService.loadFieldsFromProject(project);
        List<String> exportFields = fieldInfo.listExportFields();
        String insertTemplate = makeInsertTemplate(exportFields);

        SAXBuilder builder = SecureXMLFactory.createSAXBuilder();
        Connection conn = null;
        try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(project.getExportPath(), "export*.xml")) {
            conn = openDbConnection();

            Iterator<Path> it = dirStream.iterator();
            while (it.hasNext()) {
                Path exportPath = it.next();
                outputLogger.info("Indexing file: {}", exportPath.getFileName());
                Document doc = builder.build(exportPath.toFile());
                indexDocument(doc, conn, insertTemplate, exportFields);
            }
        } catch (IOException e) {
            throw new MigrationException("Failed to read export files: " + e.getMessage());
        } catch (SQLException e) {
            throw new MigrationException("Failed to update database: " + e.getMessage());
        } catch (JDOMException e) {
            throw new MigrationException("Failed to parse export file: " + e.getMessage());
        } finally {
            closeDbConnection(conn);
        }

        project.getProjectProperties().setIndexedDate(Instant.now());
        ProjectPropertiesSerialization.write(project);
    }

    private void assertCollectionExported() {
        if (project.getProjectProperties().getExportedDate() == null) {
            throw new InvalidProjectStateException("Must complete an export of the collection prior to indexing");
        }
    }

    private String makeInsertTemplate(List<String> exportFields) {
        return "insert into " + TB_NAME + " values ("
                + exportFields.stream().map(f -> "?").collect(Collectors.joining(","))
                + ")";
    }

    private void indexDocument(Document doc, Connection conn, String insertTemplate, List<String> exportFields)
            throws SQLException {
        Element root = doc.getRootElement();
        List<Element> recordEls = root.getChildren("record");
        for (Element recordEl : recordEls) {
            PreparedStatement stmt = conn.prepareStatement(insertTemplate);
            for (int i = 0; i < exportFields.size(); i++) {
                String exportField = exportFields.get(i);
                Element childEl = recordEl.getChild(exportField);
                if (childEl == null) {
                    throw new InvalidProjectStateException("Missing configured field " + exportField
                            + " in export document, aborting indexing due to configuration mismatch");
                }
                String value = childEl.getTextTrim();
                value = value == null ? "" : value;
                stmt.setString(i + 1, value);
            }
            stmt.executeUpdate();
            stmt.close();
        }
    }

    public void createDatabase(boolean force) throws IOException {
        ensureDatabaseState(force);

        CdmFieldInfo fieldInfo = fieldService.loadFieldsFromProject(project);
        List<String> exportFields = fieldInfo.listExportFields();
        StringBuilder queryBuilder = new StringBuilder("CREATE TABLE " + TB_NAME + " (\n");
        for (int i = 0; i < exportFields.size(); i++) {
            String field = exportFields.get(i);
            queryBuilder.append(field).append(' ');
            if (CdmFieldInfo.CDM_ID.equals(field)) {
                queryBuilder.append("INT PRIMARY KEY NOT NULL");
            } else {
                queryBuilder.append("TEXT");
            }
            if (i < exportFields.size() - 1) {
                queryBuilder.append(',');
            }
        }
        queryBuilder.append(')');
        log.debug("Creating database with query: {}", queryBuilder);

        Connection conn = null;
        try {
            conn = openDbConnection();
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(queryBuilder.toString());
        } catch (SQLException e) {
            throw new MigrationException("Failed to create table: " + e.getMessage(), e);
        } finally {
            closeDbConnection(conn);
        }
    }

    private void ensureDatabaseState(boolean force) {
        if (Files.exists(project.getIndexPath())) {
            if (force) {
                try {
                    Files.delete(project.getIndexPath());
                } catch (IOException e) {
                    throw new MigrationException("Failed to overwrite index file", e);
                }
            } else {
                throw new StateAlreadyExistsException("Cannot create index, an index file already exists."
                        + " Use the force flag to overwrite.");
            }
        }
    }

    public void setFieldService(CdmFieldService fieldService) {
        this.fieldService = fieldService;
    }

    public void setProject(MigrationProject project) {
        this.project = project;
    }

    public Connection openDbConnection() throws SQLException {
        try {
            Class.forName("org.sqlite.JDBC");
            return DriverManager.getConnection("jdbc:sqlite:" + project.getIndexPath());
        } catch (ClassNotFoundException e) {
            throw new MigrationException("Failed to open database connection to " + project.getIndexPath(), e);
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

    /**
     * Remove the index and related properties
     */
    public void removeIndex() {
        try {
            Files.delete(project.getIndexPath());
        } catch (NoSuchFileException e) {
            log.debug("Index file does not exist, skipping deletion");
        } catch (IOException e) {
            log.error("Failed to delete index", e);
        }
        // Clear indexed date property in case it was set
        try {
            project.getProjectProperties().setIndexedDate(null);
            ProjectPropertiesSerialization.write(project);
        } catch (IOException e) {
            log.error("Failed to delete index", e);
        }

    }
}