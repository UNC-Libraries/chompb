package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.AltTextInfo;
import edu.unc.lib.boxc.migration.cdm.model.AltTextInfo.AltTextMapping;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Service for interacting with alt-text
 * @author krwong
 */
public class AltTextService {
    private static final Logger log = LoggerFactory.getLogger(AltTextService.class);

    private MigrationProject project;
    private CdmIndexService indexService;
    private List<String> ids;
    private AltTextInfo altTextInfo;

    public AltTextService() {
    }

    /**
     * Generate the alt-text mapping template
     * @throws Exception
     */
    public void generateAltTextMapping() throws Exception {
        assertProjectStateValid();
        Path mappingPath = getMappingPath();
        BufferedWriter writer = Files.newBufferedWriter(mappingPath);

        try (var csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT
                .withHeader(AltTextInfo.CSV_HEADERS));) {
            for (var id : getIds()) {
                csvPrinter.printRecord(id, null);
            }
        }

        setUpdatedDate(Instant.now());
    }

    protected void setUpdatedDate(Instant timestamp) throws IOException {
        project.getProjectProperties().setAltTextFilesUpdatedDate(timestamp);
        ProjectPropertiesSerialization.write(project);
    }

    public Path getMappingPath() {
        return project.getAltTextMappingPath();
    }

    private List<String> getIds() {
        if (ids == null) {
            ids = new ArrayList<>();
            // for all file objects in the project (exclude grouped objects, compound objects, pdf objects)
            String query = "select " + CdmFieldInfo.CDM_ID + " from " + CdmIndexService.TB_NAME
                + " where " + CdmIndexService.ENTRY_TYPE_FIELD + " = '" + CdmIndexService.ENTRY_TYPE_COMPOUND_CHILD + "'"
                + " or " + CdmIndexService.ENTRY_TYPE_FIELD + " = '" + CdmIndexService.ENTRY_TYPE_DOCUMENT_PDF + "'"
                + " or " + CdmIndexService.ENTRY_TYPE_FIELD + " is null";

            getIndexService();
            try (Connection conn = indexService.openDbConnection()) {
                var stmt = conn.createStatement();
                var rs = stmt.executeQuery(query);
                while (rs.next()) {
                    if (!rs.getString(1).isEmpty()) {
                        ids.add(rs.getString(1));
                    }
                }
                return ids;
            } catch (SQLException e) {
                throw new MigrationException("Error interacting with export index", e);
            }
        }
        return ids;
    }

    public void writeAltTextToFile(String cdmId, Path sipAltTextPath) throws IOException {
        var altTextInfo = getAltTextInfo();
        AltTextInfo.AltTextMapping altTextMapping = altTextInfo.getMappingByCdmId(cdmId);
        String altTextBody = altTextMapping.getAltTextBody();

        Files.writeString(sipAltTextPath, altTextBody, StandardCharsets.ISO_8859_1);
    }

    /**
     * @return the alt-text mapping info for the configured project
     * @throws IOException
     */
    public AltTextInfo loadMappings() throws IOException {
        return loadMappings(getMappingPath());
    }

    public static AltTextInfo loadMappings(Path mappingPath) throws IOException {
        AltTextInfo info = new AltTextInfo();
        if (Files.notExists(mappingPath)) {
            return info;
        }
        try (var csvParser = openMappingsParser(mappingPath)) {
            List<AltTextMapping> mappings = info.getMappings();
            for(CSVRecord csvRecord : csvParser) {
                mappings.add(recordToMapping(csvRecord));
            }
            return info;
        }
    }

    public static AltTextMapping recordToMapping(CSVRecord csvRecord) {
        AltTextMapping mapping = new AltTextMapping();
        mapping.setCdmId(csvRecord.get(0));
        mapping.setAltTextBody(csvRecord.get(1));
        return mapping;
    }

    /**
     * @param mappingPath Path of the CSV to read from
     * @return CSVParser for reading from the csv file
     * @throws IOException
     */
    public static CSVParser openMappingsParser(Path mappingPath) throws IOException {
        Reader reader = Files.newBufferedReader(mappingPath);
        return new CSVParser(reader, CSVFormat.DEFAULT
                .withFirstRecordAsHeader()
                .withHeader(AltTextInfo.CSV_HEADERS)
                .withTrim());
    }

    protected void assertProjectStateValid() {
        if (project.getProjectProperties().getIndexedDate() == null) {
            throw new InvalidProjectStateException("Project must be indexed prior to generating source mappings");
        }
    }

    private AltTextInfo getAltTextInfo() throws IOException {
        if (altTextInfo == null) {
            altTextInfo = loadMappings();
        }
        return altTextInfo;
    }

    private CdmIndexService getIndexService() {
        if (indexService == null) {
            indexService = new CdmIndexService();
            indexService.setProject(project);
        }
        return indexService;
    }

    public void setProject(MigrationProject project) {
        this.project = project;
    }

    public void setIndexService(CdmIndexService indexService) {
        this.indexService = indexService;
    }
}