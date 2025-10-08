package edu.unc.lib.boxc.migration.cdm.services;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import edu.unc.lib.boxc.common.util.URIUtil;
import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo.CdmFieldEntry;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;

/**
 * Service for persistence and retrieval of CDM field information
 *
 * @author bbpennel
 */
public class CdmFieldService {
    private CloseableHttpClient httpClient;
    private String cdmBaseUri;
    private MigrationProject project;
    public static final String CSV = "comma";
    public static final String EAD_TO_CDM = "ead";
    private static final String CDM_NICK_FIELD = "nick";
    private static final String CDM_NAME_FIELD = "name";
    private static final String CDM_REQUIRED_FIELD = "req";
    private static final String CDM_SEARCHABLE_FIELD = "search";
    private static final String CDM_HIDDEN_FIELD = "hide";
    private static final String CDM_VOCAB_FIELD = "vocab";
    private static final String CDM_DC_MAPPING_FIELD = "dc";

    public static final String EXPORT_NICK_FIELD = "cdm_nick";
    public static final String EXPORT_AS_FIELD = "export_as";
    public static final String EXPORT_DESC_FIELD = "description";
    public static final String EXPORT_SKIP_FIELD = "skip_export";
    public static final String EXPORT_CDM_REQUIRED = "cdm_required";
    public static final String EXPORT_CDM_SEARCHABLE = "cdm_searchable";
    public static final String EXPORT_CDM_HIDDEN = "cdm_hidden";
    public static final String EXPORT_CDM_VOCAB = "cdm_vocab";
    public static final String EXPORT_CDM_DC_MAPPING = "cdm_dc_mapping";
    public static final String[] EXPORT_CSV_HEADERS = new String[] {
            EXPORT_NICK_FIELD, EXPORT_AS_FIELD, EXPORT_DESC_FIELD, EXPORT_SKIP_FIELD, EXPORT_CDM_REQUIRED,
            EXPORT_CDM_SEARCHABLE, EXPORT_CDM_HIDDEN, EXPORT_CDM_VOCAB, EXPORT_CDM_DC_MAPPING };

    public CdmFieldService() {
    }

    /**
     * Get the URL for retrieving field info for the given collection
     * @param collectionId
     * @return
     */
    public static String getFieldInfoUrl(String collectionId) {
        return "dmwebservices/index.php?q=dmGetCollectionFieldInfo/" + collectionId + "/json";
    }

    /**
     * Retrieve field information for the project's collection from CDM
     * @param collectionId
     * @return
     */
    public CdmFieldInfo retrieveFieldsForCollection(String collectionId) throws IOException {
        String infoUri = URIUtil.join(cdmBaseUri, getFieldInfoUrl(collectionId));

        CdmFieldInfo fieldInfo = new CdmFieldInfo();

        ObjectMapper mapper = new ObjectMapper();
        HttpGet getMethod = new HttpGet(infoUri);
        try (CloseableHttpResponse resp = httpClient.execute(getMethod)) {
            //Error looking up collection
            String body = IOUtils.toString(resp.getEntity().getContent(), StandardCharsets.ISO_8859_1);
            if (body.contains("Error looking up collection")) {
                throw new MigrationException("No collection with ID '" + collectionId
                        + "' found on server at " + infoUri);
            }
            JsonParser parser = mapper.getFactory().createParser(body);
            if (parser.nextToken() != JsonToken.START_ARRAY) {
                throw new MigrationException("Unexpected response from URL " + infoUri
                        + "\nIt must be a JSON array, please check the response.");
            }
            while (parser.nextToken() == JsonToken.START_OBJECT) {
                ObjectNode entryNode = mapper.readTree(parser);
                String nick = entryNode.get(CDM_NICK_FIELD).asText();
                String description = entryNode.get(CDM_NAME_FIELD).asText();
                boolean cdmRequired = entryNode.get(CDM_REQUIRED_FIELD).asBoolean();
                boolean cdmSearchable = entryNode.get(CDM_SEARCHABLE_FIELD).asBoolean();
                boolean cdmHidden = entryNode.get(CDM_HIDDEN_FIELD).asBoolean();
                boolean cdmVocab = entryNode.get(CDM_VOCAB_FIELD).asBoolean();
                String dcMapping = entryNode.get(CDM_DC_MAPPING_FIELD).asText();
                CdmFieldEntry fieldEntry = new CdmFieldEntry();
                fieldEntry.setNickName(nick);
                fieldEntry.setExportAs(nick);
                fieldEntry.setDescription(description);
                fieldEntry.setSkipExport(false);
                fieldEntry.setCdmRequired(booleanToString(cdmRequired));
                fieldEntry.setCdmSearchable(booleanToString(cdmSearchable));
                fieldEntry.setCdmHidden(booleanToString(cdmHidden));
                fieldEntry.setCdmVocab(booleanToString(cdmVocab));
                fieldEntry.setCdmDcMapping(dcMapping);
                fieldInfo.getFields().add(fieldEntry);
            }
        } catch (JsonParseException e) {
            throw new MigrationException("Failed to parse response from URL " + infoUri, e);
        }
        return fieldInfo;
    }

    /**
     * Persist the field information out to the project
     * @param project
     * @param fieldInfo
     * @throws IOException
     */
    public void persistFieldsToProject(MigrationProject project, CdmFieldInfo fieldInfo) throws IOException {
        Path fieldsPath = project.getFieldsPath();
        try (
            BufferedWriter writer = Files.newBufferedWriter(fieldsPath);
            CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT
                    .withHeader(EXPORT_CSV_HEADERS));
        ) {
            for (CdmFieldEntry entry : fieldInfo.getFields()) {
                csvPrinter.printRecord(entry.getNickName(), entry.getExportAs(), entry.getDescription(),
                        entry.getSkipExport(), entry.getCdmRequired(), entry.getCdmSearchable(),
                        entry.getCdmHidden(), entry.getCdmVocab(), entry.getCdmDcMapping());
            }
        }
    }

    /**
     * Retrieve field information from the listing captured in the given project
     * @param project
     * @return
     * @throws IOException
     */
    public CdmFieldInfo loadFieldsFromProject(MigrationProject project) {
        Path fieldsPath = project.getFieldsPath();
        try (
            Reader reader = Files.newBufferedReader(fieldsPath);
            CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                    .withFirstRecordAsHeader()
                    .withHeader(EXPORT_CSV_HEADERS)
                    .withTrim());
        ) {
            CdmFieldInfo fieldInfo = new CdmFieldInfo();
            List<CdmFieldEntry> fields = fieldInfo.getFields();
            for (CSVRecord csvRecord : csvParser) {
                CdmFieldEntry entry = new CdmFieldEntry();
                entry.setNickName(csvRecord.get(0));
                entry.setExportAs(csvRecord.get(1));
                entry.setDescription(csvRecord.get(2));
                entry.setSkipExport(Boolean.parseBoolean(csvRecord.get(3)));
                entry.setCdmRequired(csvRecord.get(4));
                entry.setCdmSearchable(csvRecord.get(5));
                entry.setCdmHidden(csvRecord.get(6));
                entry.setCdmVocab(csvRecord.get(7));
                entry.setCdmDcMapping(csvRecord.get(8));
                fields.add(entry);
            }
            return fieldInfo;
        } catch (IOException e) {
            throw new InvalidProjectStateException("Cannot read fields file", e);
        }
    }

    /**
     * Validate the field file for the given project, throwing InvalidProjectStateException if not.
     * @param project
     */
    public void validateFieldsFile(MigrationProject project) {
        Path fieldsPath = project.getFieldsPath();
        Set<String> nickFields = new HashSet<>();
        Set<String> exportFields = new HashSet<>();
        try (
            Reader reader = Files.newBufferedReader(fieldsPath);
            CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                    .withFirstRecordAsHeader()
                    .withHeader(EXPORT_CSV_HEADERS)
                    .withTrim());
        ) {
            int line = 2;
            for (CSVRecord csvRecord : csvParser) {
                if (csvRecord.size() != 9) {
                    throw new InvalidProjectStateException(
                            "Invalid CDM fields entry at line " + line);
                }
                validateFieldName(csvRecord.get(0), EXPORT_NICK_FIELD, line, nickFields);
                validateFieldName(csvRecord.get(1), EXPORT_AS_FIELD, line, exportFields);
                line++;
            }
            if (line == 2) {
                throw new InvalidProjectStateException("CDM fields file is empty, it must contain at least 1 entry");
            }
        } catch (NoSuchFileException e) {
            throw new InvalidProjectStateException("CDM fields file is missing, expected at path", e);
        } catch (IOException e) {
            throw new InvalidProjectStateException("Cannot read fields file", e);
        }
    }

    private void validateFieldName(String field, String headerField, int line, Set<String> existing) {
        if (StringUtils.isBlank(field)) {
            throw new InvalidProjectStateException("Empty " + headerField + " value at line " + line);
        }
        if (!field.matches("[A-Za-z0-9_]+")) {
            throw new InvalidProjectStateException("Invalid " + headerField + " value '" + field + "' at line " + line);
        }
        if (existing.contains(field)) {
            throw new InvalidProjectStateException("Duplicate " + headerField + " value '"
                    + field + "' at line " + line + ", values must be unique");
        }
        existing.add(field);
    }


    /**
     * Retrieve field information for the project from the CSV or TSV
     * @param exportedObjectsPath path of the CSV or TSV
     * @param source CSV or tab-delimited EAD to CDM file
     * @return CDM Field Info object
     */
    public CdmFieldInfo retrieveFields(Path exportedObjectsPath, String source) {
        CdmFieldInfo fieldInfo = new CdmFieldInfo();

        try {
            Reader reader = Files.newBufferedReader(exportedObjectsPath);
            var format = CSVFormat.DEFAULT;
            if (Objects.equals(source, EAD_TO_CDM)) {
                format = CSVFormat.TDF;
            }
            var csvFormat = format.builder().setQuote(null).setTrim(true).get();
            var parser = CSVParser.parse(reader, csvFormat);

            List<String> headers = parser.getRecords().getFirst().toList();
            for (String header : headers) {
                CdmFieldEntry fieldEntry = new CdmFieldEntry();
                var formattedHeader = header.replaceAll(" ", "_").replaceAll("\"", "").toLowerCase();
                fieldEntry.setNickName(formattedHeader);
                fieldEntry.setExportAs(formattedHeader);
                fieldEntry.setDescription(formattedHeader);
                fieldEntry.setSkipExport(false);
                fieldInfo.getFields().add(fieldEntry);
            }
        } catch (Exception e) {
            throw new MigrationException("Failed to parse exported objects path " + exportedObjectsPath, e);
        }
        return fieldInfo;
    }

    private String booleanToString(boolean bool) {
        return bool ? "y" : "n";
    }

    public void setHttpClient(CloseableHttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public void setCdmBaseUri(String cdmBaseUri) {
        this.cdmBaseUri = cdmBaseUri;
    }

    public void setProject(MigrationProject project) {
        this.project = project;
    }
}
