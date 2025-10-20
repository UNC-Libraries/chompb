package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.common.xml.SecureXMLFactory;
import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.exceptions.StateAlreadyExistsException;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo;
import edu.unc.lib.boxc.migration.cdm.options.CdmIndexOptions;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo.ID_FIELD;
import static edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo.SOURCE_FILE_FIELD;
import static edu.unc.lib.boxc.migration.cdm.services.CdmFieldService.CSV;
import static edu.unc.lib.boxc.migration.cdm.services.CdmFieldService.EAD_TO_CDM;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmHeaderConstants.CITATION;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmHeaderConstants.COLLECTION_NAME;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmHeaderConstants.COLLECTION_NUMBER;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmHeaderConstants.COLLECTION_URL;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmHeaderConstants.CONTAINER;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmHeaderConstants.CONTAINER_TYPE;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmHeaderConstants.EXTENT;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmHeaderConstants.FILENAME;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmHeaderConstants.GENRE_FORM;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmHeaderConstants.GEOGRAPHIC_NAME;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmHeaderConstants.HOOK_ID;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmHeaderConstants.LOC_IN_COLLECTION;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmHeaderConstants.OBJECT;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmHeaderConstants.OBJ_FILENAME;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmHeaderConstants.PROCESS_INFO;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmHeaderConstants.REF_ID;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmHeaderConstants.SCOPE_CONTENT;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmHeaderConstants.TSV_HEADERS;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmHeaderConstants.TSV_WITH_ID_HEADERS;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmHeaderConstants.UNIT_DATE;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmHeaderConstants.UNIT_TITLE;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Service for populating and querying the index of exported CDM records for a migration project
 * @author bbpennel
 */
public class CdmIndexService extends IndexService {
    private static final Logger log = getLogger(CdmIndexService.class);
    private static final String CLOSE_CDM_ID_TAG = "</dmrecord>";
    public static final String ENTRY_TYPE_GROUPED_WORK = "grouped_work";
    public static final String ENTRY_TYPE_COMPOUND_OBJECT = "cpd_object";
    public static final String ENTRY_TYPE_COMPOUND_CHILD = "cpd_child";
    public static final String ENTRY_TYPE_DOCUMENT_PDF = "doc_pdf";

    private static final Pattern CONTROL_PATTERN = Pattern.compile("[\\p{Cntrl}&&[^\r\n\t]]");
    private static final Pattern IGNORE_CLOSING_PATTERN = Pattern.compile(
            "(a|span|div|img|ul|li|ol|h\\d|input|label|html|table|tr|td|th)");

    private MigrationProject project;
    private CdmFieldService fieldService;
    private List<String> indexingWarnings = new ArrayList<>();

    public void index(CdmIndexOptions options) throws Exception {
        indexAll();
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
        var pdfIds = new HashSet<String>();

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
                    // Store details about where info about compound children and pdf objects can be found
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
            // Assign type information to objects, based on compound/pdf object status
            assignObjectTypeDetails(conn, cpdToIdMap, pdfIds);
            assignPdfObjectTypeDetails(conn, pdfIds);
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
        var closingTag = new StringBuilder();
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
                    if (IGNORE_CLOSING_PATTERN.matcher(closingTag.toString()).matches()) {
                        log.debug("Ignoring an html closing '{}' tag in field '{}' value", closingTag, elementName);
                        state = DescState.CONTENT;
                        content.append("</").append(closingTag).append('>');
                    } else {
                        state = DescState.OUTSIDE;
                        rootEl.addContent(new Element(elementName.toString()).setText(content.toString()));
                        elementName = new StringBuilder();
                        content = new StringBuilder();
                    }
                    closingTag = new StringBuilder();
                } else {
                    closingTag.append(c);
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
    public static final String DELETE_PDF_CHILDREN_TEMPLATE =
            "delete from " + TB_NAME + " where " + CdmFieldInfo.CDM_ID + " = ?";
    public static final String ASSIGN_PARENT_PDF_TEMPLATE =
            "update " + TB_NAME + " set " + ENTRY_TYPE_FIELD + " = '" + ENTRY_TYPE_DOCUMENT_PDF
                    + "' where " + CdmFieldInfo.CDM_ID + " = ?";

    /**
     * Add additional information to records to indicate if they are compound objects or children of one.
     * @param dbConn
     * @param cpdToIdMap
     */
    private void assignObjectTypeDetails(Connection dbConn, Map<String, String> cpdToIdMap, HashSet<String> pdfIds) {
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
                // Delete children of document-pdf objects
                if (Objects.equals(cpdRoot.getChildTextTrim("type"), "Document-PDF")) {
                    pdfIds.add(cpdId);
                    for (var pageEl : childRoot.getChildren("page")) {
                        var childId = pageEl.getChildTextTrim("pageptr");
                        try (var deleteStmt = dbConn.prepareStatement(DELETE_PDF_CHILDREN_TEMPLATE)) {
                            deleteStmt.setString(1, childId);
                            deleteStmt.executeUpdate();
                        }
                    }
                } else {
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

    /**
     * Add additional information to records to indicate if they are document-pdf objects
     * @param dbConn
     * @param pdfIds
     */
    private void assignPdfObjectTypeDetails(Connection dbConn, HashSet<String> pdfIds) {
        pdfIds.forEach(pdfId -> {
            try (var parentTypeStmt = dbConn.prepareStatement(ASSIGN_PARENT_PDF_TEMPLATE)) {
                // Assign document-pdf object type to parent object
                parentTypeStmt.setString(1, pdfId);
                parentTypeStmt.executeUpdate();
            } catch (SQLException e) {
                throw new MigrationException("Failed to update type information for " + pdfId, e);
            }
        });
    }

    public Connection openDbConnection() throws SQLException {
        try {
            Class.forName("org.sqlite.JDBC");
            return DriverManager.getConnection("jdbc:sqlite:" + project.getIndexPath());
        } catch (ClassNotFoundException e) {
            throw new MigrationException("Failed to open database connection to " + project.getIndexPath(), e);
        }
    }

    /**
     * @return Warning messages generated while indexing
     */
    public List<String> getIndexingWarnings() {
        return indexingWarnings;
    }

    public void setProject(MigrationProject project) {
        this.project = project;
    }

    public void setFieldService(CdmFieldService fieldService) {
        this.fieldService = fieldService;
    }
}
