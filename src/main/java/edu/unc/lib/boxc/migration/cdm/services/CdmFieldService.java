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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
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

    private static final String CDM_NICK_FIELD = "nick";
    private static final String CDM_NAME_FIELD = "name";

    public static final String EXPORT_NICK_FIELD = "cdm_nick";
    public static final String EXPORT_AS_FIELD = "export_as";
    public static final String EXPORT_DESC_FIELD = "description";
    public static final String EXPORT_SKIP_FIELD = "skip_export";
    public static final String[] EXPORT_CSV_HEADERS = new String[] {
            EXPORT_NICK_FIELD, EXPORT_AS_FIELD, EXPORT_DESC_FIELD, EXPORT_SKIP_FIELD };

    public CdmFieldService() {
    }

    /**
     * Retrieve field information for the project's collection from CDM
     * @param project
     * @return
     */
    public CdmFieldInfo retrieveFieldsForCollection(MigrationProject project) throws IOException {
        String infoUri = URIUtil.join(cdmBaseUri, "dmwebservices/index.php?q=dmGetCollectionFieldInfo/"
                        + project.getProjectProperties().getCdmCollectionId() + "/json");

        CdmFieldInfo fieldInfo = new CdmFieldInfo();

        ObjectMapper mapper = new ObjectMapper();
        HttpGet getMethod = new HttpGet(infoUri);
        try (CloseableHttpResponse resp = httpClient.execute(getMethod)) {
            JsonParser parser = mapper.getFactory().createParser(resp.getEntity().getContent());
            if (parser.nextToken() != JsonToken.START_ARRAY) {
                throw new MigrationException("Unexpected response from URL " + infoUri
                        + "\nIt must be a JSON array, please check the response.");
            }
            while (parser.nextToken() == JsonToken.START_OBJECT) {
                ObjectNode entryNode = mapper.readTree(parser);
                String nick = entryNode.get(CDM_NICK_FIELD).asText();
                String description = entryNode.get(CDM_NAME_FIELD).asText();
                CdmFieldEntry fieldEntry = new CdmFieldEntry();
                fieldEntry.setNickName(nick);
                fieldEntry.setExportAs(nick);
                fieldEntry.setDescription(description);
                fieldEntry.setSkipExport(false);
                fieldInfo.getFields().add(fieldEntry);
            }
        } catch (JsonParseException e) {
            throw new MigrationException("Failed to parse response from URL " + infoUri
                    + ": " + e.getMessage());
        }
        return fieldInfo;
    }

    /**
     * Persist the field information out to the project project
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
                        entry.getSkipExport());
            }
        }
    }

    /**
     * Retrieve field information from the listing captured in the given project
     * @param project
     * @return
     * @throws IOException
     */
    public CdmFieldInfo loadFieldsFromProject(MigrationProject project) throws IOException {
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
                fields.add(entry);
            }
            return fieldInfo;
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
                if (csvRecord.size() != 4) {
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
            throw new InvalidProjectStateException("CDM fields file is missing, expected at path: " + e.getMessage());
        } catch (IOException e) {
            throw new InvalidProjectStateException("Cannot read fields file: " + e.getMessage());
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

    public void setHttpClient(CloseableHttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public void setCdmBaseUri(String cdmBaseUri) {
        this.cdmBaseUri = cdmBaseUri;
    }
}
