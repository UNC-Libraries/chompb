package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.AspaceRefIdInfo;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Aspace ref id service
 * @author krwong
 */
public class AspaceRefIdService {
    private static final Logger log = getLogger(AspaceRefIdService.class);

    private MigrationProject project;
    private CdmIndexService indexService;

    public AspaceRefIdService() {
    }

    /**
     * Generate the aspace ref id mapping template
     * @throws Exception
     */
    public void generateAspaceRefIdMapping() throws IOException {
        assertProjectStateValid();
        Path mappingPath = getMappingPath();
        BufferedWriter writer = Files.newBufferedWriter(mappingPath);

        try (var csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT
                .withHeader(AspaceRefIdInfo.CSV_HEADERS))) {
            for (var id : getIds()) {
                csvPrinter.printRecord(id, null);
            }
        }

        setUpdatedDate(Instant.now());
    }

    protected void setUpdatedDate(Instant timestamp) throws IOException {
        project.getProjectProperties().setAspaceRefIdMappingsUpdatedDate(timestamp);
        ProjectPropertiesSerialization.write(project);
    }

    public Path getMappingPath() {
        return project.getAspaceRefIdMappingPath();
    }

    private List<String> getIds() {
        List<String> ids = new ArrayList<>();
        // for all work objects in the project (grouped works, compound objects, and single file works)
        String query = "select " + CdmFieldInfo.CDM_ID
                + " from " + CdmIndexService.TB_NAME
                + " where " + CdmIndexService.ENTRY_TYPE_FIELD + " = '" + CdmIndexService.ENTRY_TYPE_GROUPED_WORK + "'"
                + " or " + CdmIndexService.ENTRY_TYPE_FIELD + " = '" + CdmIndexService.ENTRY_TYPE_COMPOUND_OBJECT + "'"
                + " or " + CdmIndexService.ENTRY_TYPE_FIELD + " = '" + CdmIndexService.ENTRY_TYPE_DOCUMENT_PDF + "'"
                + " or " + CdmIndexService.ENTRY_TYPE_FIELD + " is null"
                + " and " + CdmIndexService.PARENT_ID_FIELD + " is null";

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

    /**
     * @return the aspace ref id mapping info for the configured project
     * @throws IOException
     */
    public AspaceRefIdInfo loadMappings() throws IOException {
        return loadMappings(getMappingPath());
    }

    public static AspaceRefIdInfo loadMappings(Path mappingPath) throws IOException {
        AspaceRefIdInfo info = new AspaceRefIdInfo();
        if (Files.notExists(mappingPath)) {
            return info;
        }
        try (var csvParser = openMappingsParser(mappingPath)) {
            Map<String, String> mappings = info.getMappings();
            for(CSVRecord csvRecord : csvParser) {
                mappings.put(csvRecord.get(0), csvRecord.get(1));
            }
            return info;
        }
    }

    /**
     * @param mappingsPath Path of the CSV to read from
     * @return CSVParser for reading from the csv file
     * @throws IOException
     */
    public static CSVParser openMappingsParser(Path mappingsPath) throws IOException {
        Reader reader = Files.newBufferedReader(mappingsPath);
        return new CSVParser(reader, CSVFormat.DEFAULT
                .withFirstRecordAsHeader()
                .withHeader(AspaceRefIdInfo.CSV_HEADERS)
                .withTrim());
    }

    private CdmIndexService getIndexService() {
        if (indexService == null) {
            indexService = new CdmIndexService();
            indexService.setProject(project);
        }
        return indexService;
    }

    protected void assertProjectStateValid() {
        if (project.getProjectProperties().getIndexedDate() == null) {
            throw new InvalidProjectStateException("Project must be indexed prior to generating source mappings");
        }
    }

    public void setProject(MigrationProject project) {
        this.project = project;
    }

    public void setIndexService(CdmIndexService indexService) {
        this.indexService = indexService;
    }
}
