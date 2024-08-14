package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.common.xml.SecureXMLFactory;
import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.exceptions.StateAlreadyExistsException;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.CdmIndexOptions;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.slf4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Reader;
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
import java.util.Objects;
import java.util.regex.Pattern;
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
    public static final String CHILD_ORDER_FIELD = "cdm2bxc_order";
    public static final String ENTRY_TYPE_GROUPED_WORK = "grouped_work";
    public static final String ENTRY_TYPE_COMPOUND_OBJECT = "cpd_object";
    public static final String ENTRY_TYPE_COMPOUND_CHILD = "cpd_child";
    public static final List<String> MIGRATION_FIELDS = Arrays.asList(
            PARENT_ID_FIELD, ENTRY_TYPE_FIELD, CHILD_ORDER_FIELD);
    private static final Pattern CONTROL_PATTERN = Pattern.compile("[\\p{Cntrl}&&[^\r\n\t]]");

    private MigrationProject project;
    private CdmFieldService fieldService;

    private String recordInsertSqlTemplate;
    private List<String> indexingWarnings = new ArrayList<>();

    public void index(CdmIndexOptions options) throws Exception {
        if (options.getCsvFile() != null) {
            indexAllFromCsv(options);
        } else {
            indexAll();
        }
    }

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

        var descAllPath = CdmFileRetrievalService.getDescAllPath(project);
        try (
                var conn = openDbConnection();
                var lineStream = Files.lines(descAllPath);
        ) {
            // Compile the lines belonging to a record, wrap in record tags
            var recordBuilder = new StringBuilder();
            var incompleteRecord = false;
            for (var line: (Iterable<String>) lineStream::iterator) {
                recordBuilder.append(line).append('\n');
                incompleteRecord = true;
                // reached the end of a record
                if (line.contains(CLOSE_CDM_ID_TAG)) {
                    Document doc = buildDocument(recordBuilder.toString());
                    // Store details about where info about compound children can be found
                    recordIfCompoundObject(doc, cpdToIdMap);
                    indexDocument(doc, conn, fieldInfo);
                    // reset the record builder for the next record
                    recordBuilder = new StringBuilder();
                    incompleteRecord = false;
                }
            }
            if (incompleteRecord) {
                throw new MigrationException("Failed to parse desc.all file, incomplete record with body:\n" +
                        recordBuilder);
            }
            // Assign type information to objects, based on compound object status
            assignObjectTypeDetails(conn, cpdToIdMap);
        } catch (IOException e) {
            throw new MigrationException("Failed to read export files", e);
        } catch (SQLException e) {
            throw new MigrationException("Failed to update database", e);
        }

        project.getProjectProperties().setIndexedDate(Instant.now());
        ProjectPropertiesSerialization.write(project);
    }

    private enum DescState {
        OUTSIDE, OPENING, CONTENT, START_CLOSE, CLOSING;
    }

    protected Document buildDocument(String body) {
        // Trim out control characters, aside from newlines, carriage returns and tabs
        body = CONTROL_PATTERN.matcher(body).replaceAll("");
        var rootEl = new Element("record");
        var doc = new Document().setRootElement(rootEl);
        var state = DescState.OUTSIDE;
        var elementName = new StringBuilder();
        var content = new StringBuilder();
        // Extract elements from body using a state machine since a regex caused stackoverflow errors.
        // Reconstituting the document using jdom components to handle XML escaping, which CDM does not do.
        for (int i = 0; i < body.length(); i++) {
            char c = body.charAt(i);
            switch(state) {
            case OUTSIDE:
                if (c == '<') {
                    state = DescState.OPENING;
                }
                break;
            case OPENING:
                if (c == '>') {
                    state = DescState.CONTENT;
                } else {
                    elementName.append(c);
                }
                break;
            case CONTENT:
                if (c == '<') {
                    state = DescState.START_CLOSE;
                } else {
                    content.append(c);
                }
                break;
            case START_CLOSE:
                if (c == '/') {
                    state = DescState.CLOSING;
                } else if (c == '<') {
                    // Handling extra < right before closing tag
                    content.append(c);
                } else {
                    // It was a stray <, revert to content mode
                    state = DescState.CONTENT;
                    content.append('<').append(c);
                }
                break;
            case CLOSING:
                if (c == '>') {
                    state = DescState.OUTSIDE;
                    rootEl.addContent(new Element(elementName.toString()).setText(content.toString()));
                    elementName = new StringBuilder();
                    content = new StringBuilder();
                }
                break;
            }
        }
        return doc;
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
                    + PARENT_ID_FIELD + " = ?,"
                    + CHILD_ORDER_FIELD + " = ?"
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
                var childRoot = cpdRoot;
                // Monograph objects have a slightly different structure
                if (Objects.equals(cpdRoot.getChildTextTrim("type"), "Monograph")) {
                    childRoot = cpdRoot.getChild("node");
                }
                // Assign each child object to its parent compound
                int orderId = 0;
                for (var pageEl : childRoot.getChildren("page")) {
                    var childId = pageEl.getChildTextTrim("pageptr");
                    try (var childStmt = dbConn.prepareStatement(ASSIGN_CHILD_INFO_TEMPLATE)) {
                        childStmt.setString(1, cpdId);
                        childStmt.setInt(2, orderId);
                        childStmt.setString(3, childId);
                        childStmt.executeUpdate();
                    }
                    orderId++;
                }

            } catch (FileNotFoundException e) {
                var msg = "CPD file referenced by object " + cpdId + " in desc.all was not found, skipping: " + cpdPath;
                indexingWarnings.add(msg);
                log.warn(msg);
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
     * @param options
     * @throws IOException
     */
    public void createDatabase(CdmIndexOptions options) throws IOException {
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

    private String indexFieldType(String exportField) {
        if (CdmFieldInfo.CDM_ID.equals(exportField)) {
            return "INT PRIMARY KEY NOT NULL";
        } else if (CHILD_ORDER_FIELD.equals(exportField)) {
            return "INT";
        } else {
            return "TEXT";
        }
    }

    /**
     * Indexes all exported objects for this project
     * @param options
     * @throws IOException
     */
    public void indexAllFromCsv(CdmIndexOptions options) throws IOException {
        assertCsvImportExists(options);

        CdmFieldInfo fieldInfo = fieldService.loadFieldsFromProject(project);
        List<String> exportFields = fieldInfo.listAllExportFields();
        exportFields.addAll(MIGRATION_FIELDS);
        recordInsertSqlTemplate = makeInsertTemplate(exportFields);

        try (
                var conn = openDbConnection();
                Reader reader = Files.newBufferedReader(options.getCsvFile());
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                        .withFirstRecordAsHeader()
                        .withHeader(String.valueOf(exportFields))
                        .withTrim());
        ) {
            for (CSVRecord csvRecord : csvParser) {
                if (!csvRecord.get(0).isEmpty()) {
                    List<String> fieldValues = csvRecord.toList();
                    indexObject(conn, fieldValues);
                }
            }
        } catch (IOException e) {
            throw new MigrationException("Failed to read export files", e);
        } catch (SQLException e) {
            throw new MigrationException("Failed to update database", e);
        }

        project.getProjectProperties().setIndexedDate(Instant.now());
        ProjectPropertiesSerialization.write(project);
    }

    private void assertCsvImportExists(CdmIndexOptions options) {
        if (Files.notExists(options.getCsvFile())) {
            throw new InvalidProjectStateException("User provided csv must exist prior to indexing");
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

    /**
     * @return Warning messages generated while indexing
     */
    public List<String> getIndexingWarnings() {
        return indexingWarnings;
    }
}
