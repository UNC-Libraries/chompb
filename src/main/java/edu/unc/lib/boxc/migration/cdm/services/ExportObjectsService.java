package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.model.ExportObjectsInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProjectProperties;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

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
