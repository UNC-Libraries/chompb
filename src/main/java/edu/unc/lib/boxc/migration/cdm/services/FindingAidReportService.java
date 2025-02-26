package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.FindingAidReportOptions;
import edu.unc.lib.boxc.migration.cdm.util.DisplayProgressUtil;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;

import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;
import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.repeat;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Service for producing finding aid reports
 * @author krwong
 */
public class FindingAidReportService {
    private static final Logger log = getLogger(FindingAidReportService.class);
    protected static final String INDENT = "    ";
    private static final int MIN_LABEL_WIDTH = 20;
    public static final String FIELD_VALUES_REPORT = "_report.csv";
    public static final String HOOK_ID_REPORT = "hookid_report.csv";
    public static final String[] HOOKID_CSV_HEADERS = new String[] {FindingAidService.HOOK_ID_FIELD_DESC, "count"};

    private MigrationProject project;
    private CdmIndexService indexService;
    private CdmFieldService fieldService;

    /**
     * Generate the report of unique values and counts of a given field
     * @param options
     * @throws Exception
     */
    public void fieldCountUniqueValuesReport(FindingAidReportOptions options) throws Exception {
        String query = "select " + options.getField() + ", count(*) from " + CdmIndexService.TB_NAME
                + " group by " + options.getField() + " order by count(*) desc";

        assertProjectStateValid();
        getIndexService();
        Connection conn = null;
        BufferedWriter writer = Files.newBufferedWriter(getFieldValuesReportPath(options));
        try (var csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT
                .withHeader(options.getField() + "_value", "count"))) {
            conn = indexService.openDbConnection();
            var stmt = conn.createStatement();
            var rs = stmt.executeQuery(query);
            while (rs.next()) {
                if (!rs.getString(1).isBlank()) {
                    csvPrinter.printRecord(rs.getString(1), rs.getString(2));
                } else {
                    csvPrinter.printRecord("(blank)", rs.getString(2));
                }
            }
        } catch (SQLException e) {
            throw new MigrationException("Error interacting with export index", e);
        } finally {
            CdmIndexService.closeDbConnection(conn);
        }
    }

    /**
     * Generate the hookid report
     * Combines values from descri and contri fields (descri_contri) and lists counts
     * @throws Exception
     */
    public void hookIdReport() throws Exception {
        String query = "select " + FindingAidService.DESCRI_FIELD + "," + FindingAidService.CONTRI_FIELD + ", count(*)"
                + " from " + CdmIndexService.TB_NAME
                + " where " + FindingAidService.DESCRI_FIELD + " != ''"
                + " and " + FindingAidService.CONTRI_FIELD + " != ''"
                + " group by " + FindingAidService.DESCRI_FIELD + ", " + FindingAidService.CONTRI_FIELD
                + " order by count(*) desc";

        assertProjectStateValid();
        getIndexService();
        Connection conn = null;
        BufferedWriter writer = Files.newBufferedWriter(getHookIdReportPath());
        try (var csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(HOOKID_CSV_HEADERS))) {
            conn = indexService.openDbConnection();
            var stmt = conn.createStatement();
            var rs = stmt.executeQuery(query);
            while (rs.next()) {
                if (!rs.getString(1).isBlank() && !rs.getString(2).isBlank()) {
                    csvPrinter.printRecord(rs.getString(1) + "_" + rs.getString(2), rs.getString(3));
                }
            }
        } catch (SQLException e) {
            throw new MigrationException("Error interacting with export index", e);
        } finally {
            CdmIndexService.closeDbConnection(conn);
        }
    }

    public Path getFieldValuesReportPath(FindingAidReportOptions options) {
        return project.getProjectPath().resolve(options.getField() + FIELD_VALUES_REPORT);
    }

    public Path getHookIdReportPath() {
        return project.getProjectPath().resolve(HOOK_ID_REPORT);
    }

    /**
     * Report to assess collection for finding aid associations
     */
    public void collectionReport() {
        assertProjectStateValid();
        int totalRecords = countRecords("select count(*)" + " from " + CdmIndexService.TB_NAME);

        if (project.getProjectProperties().getHookId() != null
                || project.getProjectProperties().getCollectionNumber() != null) {
            String hookId = project.getProjectProperties().getHookId();
            String collectionNumber = project.getProjectProperties().getCollectionNumber();

            List<String> uniqueCollectionIds = listCollectionIds();
            int collectionIdRecords = countRecords("select count(*)"
                    + " from " + CdmIndexService.TB_NAME + " where " + FindingAidService.DESCRI_FIELD + " != ''");
            int hookIdRecords = countRecords("select count(*)"
                    + " from " + CdmIndexService.TB_NAME + " where " + FindingAidService.CONTRI_FIELD + " != ''");

            showField("Hook id", hookId);
            showField("Collection number", collectionNumber);
            showFieldListValues(uniqueCollectionIds);
            showFieldWithPercent("Records with collection ids", collectionIdRecords, totalRecords);
            showFieldWithPercent("Records with hook ids", hookIdRecords, totalRecords);
        } else {
            outputLogger.info("{}{}", INDENT, "Collection number is not set.");
            outputLogger.info("{}{}", INDENT, "Possible collection numbers:");
            Map<String, List<String>> potentialCollectionIds = listPotentialCollectionIds();
            if (potentialCollectionIds.isEmpty()) {
                outputLogger.info("no potential collection ids found");
            } else {
                showFieldMapValues(potentialCollectionIds);
            }
        }

        outputLogger.info("{}{}", INDENT, "Fields populated:");
        List<String> fieldsToReport = Arrays.asList("collec", "descri", "findin", "locati", "title", "prefer",
                "creato", "contri", "relatid");
        // check project's list of fields and make sure field to report exists in project before querying database
        fieldService.validateFieldsFile(project);
        CdmFieldInfo fieldInfo = fieldService.loadFieldsFromProject(project);
        List<String> exportFields = fieldInfo.listAllExportFields();

        for (String field : fieldsToReport) {
            int fieldRecords = 0;
            if (exportFields.contains(field)) {
                String query = "select count(*) from " + CdmIndexService.TB_NAME + " where " + field + " != ''";
                fieldRecords = countRecords(query);
            }
            outputLogger.info(DisplayProgressUtil.displayProgressWithLabel(field, fieldRecords, totalRecords));
        }
    }

    /**
     * List of unique collection id values
     */
    private List<String> listCollectionIds() {
        String collectionIdQuery = "select distinct " + FindingAidService.DESCRI_FIELD
                + " from " + CdmIndexService.TB_NAME;
        List<String> uniqueCollectionIds = new ArrayList<>();

        try (Connection conn = indexService.openDbConnection()) {
            var stmt = conn.createStatement();
            var rs = stmt.executeQuery(collectionIdQuery);
            while (rs.next()) {
                if (!rs.getString(1).isBlank()) {
                    uniqueCollectionIds.add(rs.getString(1));
                }
            }
            return uniqueCollectionIds;
        } catch (SQLException e) {
            throw new MigrationException("Error interacting with export index", e);
        }
    }

    /**
     * Count records for a given query
     */
    private Integer countRecords(String query) {
        try (Connection conn = indexService.openDbConnection()) {
            var stmt = conn.createStatement();
            var rs = stmt.executeQuery(query);
            rs.next();
            return rs.getInt(1);
        } catch (SQLException e) {
            throw new MigrationException("Error interacting with export index", e);
        }
    }

    /**
     * Map of potential collection id values and the fields they come from
     */
    private Map<String, List<String>> listPotentialCollectionIds() {
        fieldService.validateFieldsFile(project);
        CdmFieldInfo fieldInfo = fieldService.loadFieldsFromProject(project);
        List<String> exportFields = fieldInfo.listAllExportFields();

        Map<String, List<String>> potentialCollectionIds = new HashMap<>();
        // only collect the first 10 potential ids
        try (Connection conn = indexService.openDbConnection()) {
            var stmt = conn.createStatement();
            for (String field : exportFields) {
                List<String> ids = new ArrayList<>();
                ResultSet rs = stmt.executeQuery(" select " + field + " from " + CdmIndexService.TB_NAME
                        + " where " + field + " like " + "'^[A-Za-z0-9]{5}-?z?$' limit 10");
                while (rs.next()) {
                    if (!rs.getString(1).isBlank()) {
                        ids.add(rs.getString(1));
                    }
                }
                if (!ids.isEmpty()) {
                    potentialCollectionIds.put(field, ids);
                }
            }
        } catch (SQLException e) {
            throw new MigrationException("Error interacting with export index", e);
        }

        return potentialCollectionIds;
    }

    protected void showField(String label, Object value) {
        int padding = MIN_LABEL_WIDTH - label.length();
        outputLogger.info("{}{}: {}{}", INDENT, label, repeat(' ', padding), value);
    }

    protected void showFieldListValues(Collection<String> values) {
        for (String value : values) {
            outputLogger.info("{}{}* {}", INDENT, INDENT, value);
        }
    }

    protected void showFieldMapValues(Map<String, List<String>> values) {
        for (Map.Entry<String, List<String>> value : values.entrySet()) {
            outputLogger.info("{}{}* {}: {}", INDENT, INDENT, value.getKey(), value.getValue());
        }
    }

    protected void showFieldWithPercent(String label, int value, int total) {
        int padding = MIN_LABEL_WIDTH - label.length();
        double percent = (double) value / total * 100;
        outputLogger.info("{}{}: {}{} ({}%)", INDENT, label, repeat(' ', padding),
                value, format("%.1f", percent));
    }

    protected void assertProjectStateValid() {
        if (project.getProjectProperties().getIndexedDate() == null) {
            throw new InvalidProjectStateException("Project must be indexed prior to generating finding aid reports");
        }
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

    public void setFieldService(CdmFieldService fieldService) {
        this.fieldService = fieldService;
    }
}
