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
import java.util.HashMap;
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
    private CdmFieldService fieldService;
    private CdmIndexService indexService;
    private Path hookIdRefIdMapPath;
    private Boolean projectHasContriAndDescri = null;

    public static final String[] HOOKID_REFID_CSV_HEADERS = {"cache_hookid", "normalized_cache_hookid", "collid",
            "ref_id", "ao_title", "tc_type", "tc_indicator", "sc_type", "sc_indicator", "gc_type", "gc_indicator",
            "aspace_hookid", "cdm_alias"};

    public AspaceRefIdService() {
    }

    /**
     * Generate the blank aspace ref id mapping template
     * Only record ids populated
     * @throws Exception
     */
    public void generateBlankAspaceRefIdMapping() throws IOException {
        assertProjectStateValid();

        try (BufferedWriter writer = Files.newBufferedWriter(getMappingPath());
             var csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(AspaceRefIdInfo.CSV_HEADERS))) {
            for (var id : getIds()) {
                csvPrinter.printRecord(id, null);
            }
        }

        setUpdatedDate(Instant.now());
    }

    /**
     * Generate the aspace ref id mapping using the hookid_to_refid_mapping.csv
     * Record ids and ref ids populated
     * @throws Exception
     */
    public void generateAspaceRefIdMappingFromHookIdRefIdCsv() throws IOException {
        assertProjectStateValid();

        if (!hasProjectContriAndDescriFields()) {
            throw new InvalidProjectStateException("Project has no contri and descri fields");
        }

        try (BufferedWriter writer = Files.newBufferedWriter(getMappingPath());
             var csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(AspaceRefIdInfo.CSV_HEADERS))) {
            Map<String, String> idsAndHookIds = getIdsAndHookIds();
            Map<String, String> hookIdsAndRefIds = getHookIdsAndRefIds(hookIdRefIdMapPath);

            for (Map.Entry<String, String> entry : idsAndHookIds.entrySet()) {
                String recordId = entry.getKey();
                String hookId = entry.getValue();
                if (hookIdsAndRefIds.containsKey(hookId)) {
                    csvPrinter.printRecord(recordId, hookIdsAndRefIds.get(hookId));
                } else {
                    csvPrinter.printRecord(recordId, null);
                }
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

    private Map<String,String> getIdsAndHookIds() {
        Map<String, String> idsAndHookIds = new HashMap<>();

        String query = "select " + CdmFieldInfo.CDM_ID + "," + FindingAidService.DESCRI_FIELD + ","
                + FindingAidService.CONTRI_FIELD
                + " from " + CdmIndexService.TB_NAME
                + " where " + FindingAidService.DESCRI_FIELD + " is not null"
                + " and " + FindingAidService.CONTRI_FIELD + " is not null";

        getIndexService();
        try (Connection conn = indexService.openDbConnection()) {
            var stmt = conn.createStatement();
            var rs = stmt.executeQuery(query);
            while (rs.next()) {
                // if dmrecord, descri, and contri fields are not blank,
                // add dmrecord and descri_contri (hook id when combined) to map
                if (!rs.getString(1).isBlank() && !rs.getString(2).isBlank() && !rs.getString(3).isBlank()) {
                    idsAndHookIds.put(rs.getString(1), rs.getString(2) + "_" + rs.getString(3));
                }
            }
            return idsAndHookIds;
        } catch (SQLException e) {
            throw new MigrationException("Error interacting with export index", e);
        }
    }

    private Map<String, String> getHookIdsAndRefIds(Path mappingPath) throws IOException {
        if (!Files.exists(mappingPath)) {
            throw new InvalidProjectStateException(mappingPath + " does not exist");
        }

        try (var csvParser = openHookIdRefIdCsvParser(mappingPath)) {
            Map<String, String> hookIdsAndRefIds = new HashMap<>();

            for (CSVRecord csvRecord : csvParser) {
                // if cache_hook_id and ref_id are not blank, add cache_hook_id and ref_id to map
                if (!csvRecord.get(0).isBlank() && !csvRecord.get(3).isBlank()) {
                    hookIdsAndRefIds.put(csvRecord.get(0), csvRecord.get(3));
                // if normalized_cache_id and ref_id are not blank, add normalized_cache_id and ref_id to map
                } else if (!csvRecord.get(1).isBlank() && !csvRecord.get(3).isBlank()) {
                    hookIdsAndRefIds.put(csvRecord.get(1), csvRecord.get(3));
                }
            }
            return hookIdsAndRefIds;
        }
    }

    private boolean hasProjectContriAndDescriFields() {
        if (projectHasContriAndDescri == null) {
            // check if project has contri field and descri field
            fieldService.validateFieldsFile(project);
            CdmFieldInfo fieldInfo = fieldService.loadFieldsFromProject(project);
            List<String> exportFields = fieldInfo.listAllExportFields();
            projectHasContriAndDescri = exportFields.contains(FindingAidService.CONTRI_FIELD)
                    && exportFields.contains(FindingAidService.DESCRI_FIELD);
        }
        return projectHasContriAndDescri;
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

    /**
     * @param mappingPath Path of the CSV to read from
     * @return CSVParser for reading from the hookid_to_refid_map.csv file
     * @throws IOException
     */
    public static CSVParser openHookIdRefIdCsvParser(Path mappingPath) throws IOException {
        Reader reader = Files.newBufferedReader(mappingPath);
        return new CSVParser(reader, CSVFormat.DEFAULT
                .withFirstRecordAsHeader()
                .withHeader(HOOKID_REFID_CSV_HEADERS)
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

    public void setFieldService(CdmFieldService fieldService) {
        this.fieldService = fieldService;
    }

    public void setIndexService(CdmIndexService indexService) {
        this.indexService = indexService;
    }

    public void setHookIdRefIdMapPath(Path hookIdRefIdMapPath) {
        this.hookIdRefIdMapPath = hookIdRefIdMapPath;
    }
}
