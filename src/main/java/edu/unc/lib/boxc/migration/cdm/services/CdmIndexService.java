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

import edu.unc.lib.boxc.common.xml.SecureXMLFactory;
import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.exceptions.StateAlreadyExistsException;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import org.apache.commons.lang3.StringUtils;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.slf4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Service for populating and querying the index of exported CDM records for a migration project
 * @author bbpennel
 */
public class CdmIndexService {
    private static final Logger log = getLogger(CdmIndexService.class);
    private static final String CLOSE_CDM_ID_TAG = "</dmrecord>";
    public static final String TB_NAME = "cdm_records";
    public static final String PARENT_ID_FIELD = "cdm2bxc_parent_id";
    public static final String ENTRY_TYPE_FIELD = "cdm2bxc_entry_type";
    public static final String ENTRY_TYPE_GROUPED_WORK = "grouped_work";
    public static final String ENTRY_TYPE_COMPOUND_OBJECT = "cpd_object";
    public static final String ENTRY_TYPE_COMPOUND_CHILD = "cpd_child";
    public static final List<String> MIGRATION_FIELDS = Arrays.asList(PARENT_ID_FIELD, ENTRY_TYPE_FIELD);

    private MigrationProject project;
    private CdmFieldService fieldService;

    private String recordInsertSqlTemplate;

    /**
     * Indexes all exported CDM records for this project
     * @throws IOException
     */
    public void indexAll() throws IOException {
        assertCollectionExported();

        CdmFieldInfo fieldInfo = fieldService.loadFieldsFromProject(project);
        List<String> exportFields = fieldInfo.listAllExportFields();
        // Add extra migration fields into list of cdm fields in order to generate the insert template
        List<String> allFields = new ArrayList<>(exportFields);
        allFields.addAll(MIGRATION_FIELDS);
        recordInsertSqlTemplate = makeInsertTemplate(allFields);

        var cpdToIdMap = new HashMap<String, String>();

        SAXBuilder builder = SecureXMLFactory.createSAXBuilder();
        var descAllPath = CdmFileRetrievalService.getDescAllPath(project);
        try (
                var conn = openDbConnection();
                var lineStream = Files.lines(descAllPath);
        ) {
            // Compile the lines belonging to a record, wrap in record tags
            var recordBuilder = new StringBuilder("<record>");
            var incompleteRecord = false;
            for (var line: (Iterable<String>) lineStream::iterator) {
                // Ampersands are not escaped in CDM's pseudo-XML, which causes problems when building XML
                recordBuilder.append(line.replaceAll("&", "&amp;"));
                incompleteRecord = true;
                // reached the end of a record
                if (line.contains(CLOSE_CDM_ID_TAG)) {
                    recordBuilder.append("</record>");
                    Document doc = builder.build(new ByteArrayInputStream(
                            recordBuilder.toString().getBytes(StandardCharsets.UTF_8)));
                    // Store details about where info about compound children can be found
                    recordIfCompoundObject(doc, cpdToIdMap);
                    indexDocument(doc, conn, fieldInfo);
                    // reset the record builder for the next record
                    recordBuilder = new StringBuilder("<record>");
                    incompleteRecord = false;
                }
            }
            if (incompleteRecord) {
                throw new MigrationException("Failed to parse desc.all file, incomplete record with body:\n" +
                        recordBuilder.toString());
            }
            // Assign type information to objects, based on compound object status
            assignObjectTypeDetails(conn, cpdToIdMap);
        } catch (IOException e) {
            throw new MigrationException("Failed to read export files", e);
        } catch (SQLException e) {
            throw new MigrationException("Failed to update database", e);
        } catch (JDOMException e) {
            throw new MigrationException("Failed to parse export file", e);
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

    private void indexDocument(Document doc, Connection conn, CdmFieldInfo fieldInfo)
            throws SQLException {
        Element root = doc.getRootElement();
        List<String> values = listFieldValues(root, fieldInfo.listConfiguredFields());
        indexObject(conn, values);
    }

    private void recordIfCompoundObject(Document doc, Map<String, String> cpdToIdMap) {
        var recordEl = doc.getRootElement();
        var fileValue = recordEl.getChildTextTrim(CdmFieldInfo.CDM_FILE_FIELD);
        if (StringUtils.endsWithIgnoreCase(fileValue, ".cpd")) {
            var cdmId = recordEl.getChildTextTrim(CdmFieldInfo.CDM_ID);
            cpdToIdMap.put(fileValue, cdmId);
        }
    }

    private static final String ASSIGN_PARENT_COMPOUND_TYPE_TEMPLATE =
            "update " + TB_NAME + " set " + ENTRY_TYPE_FIELD + " = '" + ENTRY_TYPE_COMPOUND_OBJECT
                    + "' where " + CdmFieldInfo.CDM_ID + " = ?";
    private static final String ASSIGN_CHILD_INFO_TEMPLATE =
            "update " + TB_NAME + " set " + ENTRY_TYPE_FIELD + " = '" + ENTRY_TYPE_COMPOUND_CHILD + "', "
                    + PARENT_ID_FIELD + " = ?"
                    + " where " + CdmFieldInfo.CDM_ID + " = ?";

    /**
     * Add additional information to records to indicate if they are compound objects or children of one.
     * @param dbConn
     * @param cpdToIdMap
     */
    private void assignObjectTypeDetails(Connection dbConn, Map<String, String> cpdToIdMap) {
        SAXBuilder builder = SecureXMLFactory.createSAXBuilder();
        var cpdsPath = CdmFileRetrievalService.getExportedCpdsPath(project);
        cpdToIdMap.forEach((cpdFilename, cpdId) -> {
            var cpdPath = cpdsPath.resolve(cpdFilename);
            try (var parentTypeStmt = dbConn.prepareStatement(ASSIGN_PARENT_COMPOUND_TYPE_TEMPLATE)) {
                // Assign compound object type to parent object
                parentTypeStmt.setString(1, cpdId);
                parentTypeStmt.executeUpdate();

                var cpdDoc = builder.build(cpdPath.toFile());
                var cpdRoot = cpdDoc.getRootElement();
                // Assign each child object to its parent compound
                for (var pageEl : cpdRoot.getChildren("page")) {
                    var childId = pageEl.getChildTextTrim("pageptr");
                    try (var childStmt = dbConn.prepareStatement(ASSIGN_CHILD_INFO_TEMPLATE)) {
                        childStmt.setString(1, cpdId);
                        childStmt.setString(2, childId);
                        childStmt.executeUpdate();
                    }
                }
            } catch (JDOMException | IOException e) {
                throw new MigrationException("Failed to parse CPD file " + cpdPath, e);
            } catch (SQLException e) {
                throw new MigrationException("Failed to update type information for " + cpdId, e);
            }
        });
    }

    private List<String> listFieldValues(Element objEl, List<String> exportFields) {
        return exportFields.stream()
                .map(exportField -> {
                    Element childEl = objEl.getChild(exportField);
                    if (childEl == null) {
                        return "";
                    } else {
                        return childEl.getTextTrim();
                    }
                }).collect(Collectors.toList());
    }

    /**
     * Indexes the metadata of an object provided via exportFieldValues and migrationFieldValues
     * @param conn
     * @param exportFieldValues Values of all configured and reserved fields which belong to the object being indexed.
     *                          Must be ordered with configured fields first, followed by reserved fields
     *                          as defined in CdmFieldInfo.RESERVED_FIELDS
     * @throws SQLException
     */
    private void indexObject(Connection conn, List<String> exportFieldValues)
            throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(recordInsertSqlTemplate)) {
            for (int i = 0; i < exportFieldValues.size(); i++) {
                String value = exportFieldValues.get(i);
                stmt.setString(i + 1, value);
            }

            stmt.executeUpdate();
        }
    }

    /**
     * Create the index database with all cdm and migration fields
     * @param force
     * @throws IOException
     */
    public void createDatabase(boolean force) throws IOException {
        ensureDatabaseState(force);

        CdmFieldInfo fieldInfo = fieldService.loadFieldsFromProject(project);
        List<String> exportFields = new ArrayList<>(fieldInfo.listAllExportFields());
        exportFields.addAll(MIGRATION_FIELDS);
        StringBuilder queryBuilder = new StringBuilder("CREATE TABLE " + TB_NAME + " (\n");
        for (int i = 0; i < exportFields.size(); i++) {
            String field = exportFields.get(i);
            queryBuilder.append('"').append(field).append("\" ");
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
            throw new MigrationException("Failed to close database connection", e);
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
