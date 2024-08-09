package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.model.ExportObjectsInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProjectProperties;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Service for generating exported_objects.csv
 * @author krwong
 */
public class ExportObjectsService {
    private static final Logger log = getLogger(ExportObjectsService.class);

    private MigrationProject project;

    /**
     * Export objects from filesystem source_files.csv mapping
     * @throws Exception
     */
    public void exportFilesystemObjects() throws Exception {
        validateProjectState();
        var sourcePath = project.getSourceFilesMappingPath();
        var exportObjectPath = getExportedObjectsPath();

        // Simultaneously read from the source_files mapping and write to the exported_objects.csv
        try (
            var sourceFilesParser = SourceFileService.openMappingsParser(sourcePath);
            var exportObjectsPrinter = openMappingsPrinter(exportObjectPath);
        ) {
            for (CSVRecord sourceFileRecord : sourceFilesParser) {
                String id = sourceFileRecord.get(0);
                String filePath = sourceFileRecord.get(2);
                String filename = FilenameUtils.getName(sourceFileRecord.get(2));
                exportObjectsPrinter.printRecord(id, filePath, filename);
            }
        }
    }

    private void validateProjectState() {
        MigrationProjectProperties props = project.getProjectProperties();
        if (props.getSourceFilesUpdatedDate() == null) {
            throw new InvalidProjectStateException("Source files must be mapped");
        }
    }

    /**
     * @return the export objects mapping info for the configured project
     * @throws IOException
     */
    public ExportObjectsInfo loadExportObjects() throws IOException {
        return loadExportObjects(getExportedObjectsPath());
    }

    /**
     * @return the export objects info for the provided project
     * @throws IOException
     */
    public static ExportObjectsInfo loadExportObjects(Path mappingPath) throws IOException {
        ExportObjectsInfo info = new ExportObjectsInfo();
        if (Files.notExists(mappingPath)) {
            return info;
        }
        try (var csvParser = openMappingsParser(mappingPath)) {
            List<ExportObjectsInfo.ExportedObject> mappings = info.getObjects();
            for (CSVRecord csvRecord : csvParser) {
                mappings.add(recordToExportFile(csvRecord));
            }
            return info;
        }
    }

    public static ExportObjectsInfo.ExportedObject recordToExportFile(CSVRecord csvRecord) {
        ExportObjectsInfo.ExportedObject exportObject = new ExportObjectsInfo.ExportedObject();
        exportObject.setRecordId(csvRecord.get(0));
        exportObject.setFilePath(csvRecord.get(1));
        exportObject.setFilename(csvRecord.get(2));
        return exportObject;
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
                .withHeader(ExportObjectsInfo.CSV_HEADERS)
                .withTrim());
    }

    /**
     * @param mappingPath Path CSV will output to
     * @return CSVPrinter for writing to specified destination
     * @throws IOException
     */
    public static CSVPrinter openMappingsPrinter(Path mappingPath) throws IOException {
        BufferedWriter writer = Files.newBufferedWriter(mappingPath);
        return new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(ExportObjectsInfo.CSV_HEADERS));
    }

    public Path getExportedObjectsPath() {
        return project.getExportObjectsPath();
    }

    public void setProject(MigrationProject project) {
        this.project = project;
    }
}
