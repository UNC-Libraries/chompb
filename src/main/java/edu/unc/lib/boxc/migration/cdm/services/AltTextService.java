package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.AltTextOptions;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Service for adding alt-text
 * @author krwong
 */
public class AltTextService {
    private static final Logger log = LoggerFactory.getLogger(AltTextService.class);

    private MigrationProject project;
    private CdmIndexService indexService;
    private List<String> ids;

    public static final String ALT_TEXT = "alt-text";
    public static final String[] ALT_TEXT_CSV_HEADERS = new String[] {
            CdmFieldInfo.CDM_ID, ALT_TEXT };

    /**
     * Generate alt-text template for a collection
     */
    public void generateTemplate() throws IOException {
        BufferedWriter writer = Files.newBufferedWriter(project.getAltTextCsvPath());
        CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT
                .withHeader(ALT_TEXT_CSV_HEADERS));

        for (var id : getIds()) {
            csvPrinter.printRecord(id);
        }
        csvPrinter.close();
    }

    /**
     * Read csv and generate alt-text text files
     */
    public void uploadCsv(AltTextOptions options) throws IOException {
        if (Files.exists(options.getAltTextCsvFile())) {
            initializeAltTextDir(project);
            Path csvPath = options.getAltTextCsvFile();
            try (
                Reader reader = Files.newBufferedReader(csvPath);
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                        .withFirstRecordAsHeader()
                        .withHeader(ALT_TEXT_CSV_HEADERS)
                        .withTrim());
            ) {
                for (CSVRecord csvRecord : csvParser) {
                    String fileId = csvRecord.get(0);
                    String fileContent = csvRecord.get(1);

                    // Check that the collection contains fileId
                    if (getIds().contains(fileId) && !fileContent.matches("")) {
                        File textFile = new File(String.valueOf(project.getAltTextPath()),
                                fileId + ".txt");
                        try (var writer = new FileWriter(textFile)) {
                            writer.write(fileContent);
                        }
                    } else {
                        log.debug("Skipping line: " + csvRecord + " no alt-text provided");
                    }
                }
            } catch (IOException e) {
                log.debug("Failed to read alt-text csv file", e);
            }
        }
    }

    private List<String> getIds() {
        if (ids == null) {
            ids = new ArrayList<>();
            String query = "select " + CdmFieldInfo.CDM_ID + " from " + CdmIndexService.TB_NAME;

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

    private void initializeAltTextDir(MigrationProject project) throws IOException {
        Files.createDirectories(project.getAltTextPath());
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
