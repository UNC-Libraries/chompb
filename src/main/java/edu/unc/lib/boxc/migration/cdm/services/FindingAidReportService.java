package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.FindingAidReportOptions;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;

import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Service for producing finding aid reports
 * @author krwong
 */
public class FindingAidReportService {
    private static final Logger log = getLogger(FindingAidReportService.class);
    public static final String FIELD_VALUES_REPORT = "_report.csv";
    public static final String HOOK_ID_REPORT = "hookid_report.csv";
    public static final String[] HOOKID_CSV_HEADERS = new String[] {FindingAidService.HOOK_ID_FIELD_DESC, "count"};

    private MigrationProject project;
    private CdmIndexService indexService;

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
                + " where " + FindingAidService.DESCRI_FIELD + " is not null "
                + " and " + FindingAidService.CONTRI_FIELD + " is not null "
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
                csvPrinter.printRecord(rs.getString(1) + "_" + rs.getString(2), rs.getString(3));
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
}
