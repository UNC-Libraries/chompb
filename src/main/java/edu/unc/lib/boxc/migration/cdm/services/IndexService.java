package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.exceptions.StateAlreadyExistsException;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.CdmIndexOptions;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import org.jdom2.Element;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.slf4j.LoggerFactory.getLogger;

public class IndexService {
    private static final Logger log = getLogger(IndexService.class);
    public static final String TB_NAME = "cdm_records";
    public static final String PARENT_ID_FIELD = "cdm2bxc_parent_id";
    public static final String ENTRY_TYPE_FIELD = "cdm2bxc_entry_type";
    public static final String CHILD_ORDER_FIELD = "cdm2bxc_order";
    public static final List<String> MIGRATION_FIELDS = Arrays.asList(
            PARENT_ID_FIELD, ENTRY_TYPE_FIELD, CHILD_ORDER_FIELD);
    public String recordInsertSqlTemplate;
    public MigrationProject project;
    public CdmFieldService fieldService;

    /**
     * Create the index database with all cdm and migration fields
     * @param options
     * @throws IOException
     */
    public void createDatabase(CdmIndexOptions options) {
        ensureDatabaseState(options.getForce());

        CdmFieldInfo fieldInfo = fieldService.loadFieldsFromProject(project);
        List<String> exportFields = fieldInfo.listAllExportFields();
        exportFields.addAll(MIGRATION_FIELDS);

        StringBuilder queryBuilder = new StringBuilder("CREATE TABLE " + TB_NAME + " (\n");
        for (int i = 0; i < exportFields.size(); i++) {
            String field = exportFields.get(i);
            queryBuilder.append('"').append(field).append("\" ")
                    .append(indexFieldType(field));
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

    /**
     * Indexes the metadata of an object provided via exportFieldValues and migrationFieldValues
     * @param conn
     * @param exportFieldValues Values of all configured and reserved fields which belong to the object being indexed.
     *                          Must be ordered with configured fields first, followed by reserved fields
     *                          as defined in CdmFieldInfo.RESERVED_FIELDS
     * @throws SQLException
     */
    public void indexObject(Connection conn, List<String> exportFieldValues)
            throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(recordInsertSqlTemplate)) {
            for (int i = 0; i < exportFieldValues.size(); i++) {
                String value = exportFieldValues.get(i);
                stmt.setString(i + 1, value);
            }

            stmt.executeUpdate();
        }
    }

    public List<String> listFieldValues(Element objEl, List<String> exportFields) {
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

    public String makeInsertTemplate(List<String> exportFields) {
        return "insert into " + TB_NAME + " values ("
                + exportFields.stream().map(f -> "?").collect(Collectors.joining(","))
                + ")";
    }

    private String indexFieldType(String exportField) {
        if (CdmFieldInfo.CDM_ID.equals(exportField)) {
            return "TEXT PRIMARY KEY NOT NULL";
        } else if (CHILD_ORDER_FIELD.equals(exportField)) {
            return "INT";
        } else {
            return "TEXT";
        }
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

    public void setRecordInsertSqlTemplate(String recordInsertSqlTemplate) {
        this.recordInsertSqlTemplate = recordInsertSqlTemplate;
    }

    public MigrationProject getProject() {
        return project;
    }

    public void setProject(MigrationProject project) {
        this.project = project;
    }

    public void setFieldService(CdmFieldService fieldService) {
        this.fieldService = fieldService;
    }
}
