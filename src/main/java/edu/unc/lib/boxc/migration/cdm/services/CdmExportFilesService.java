package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProjectProperties;
import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo.SourceFileMapping;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Service for exporting source files from CDM to a chompb project for ingest
 *
 * @author bbpennel
 */
public class CdmExportFilesService {
    private static final Logger log = getLogger(CdmExportFilesService.class);

    private CdmFileRetrievalService fileRetrievalService;
    private MigrationProject project;
    private SourceFileService sourceFileService;
    private CdmIndexService indexService;

    /**
     * Export files from CDM associated with items that do not already have source files mapped
     * @return Result message if any issues were encountered, otherwise null.
     * @throws IOException
     */
    public String exportUnmapped() throws IOException {
        validateProjectState();

        var failedToDownload = new AtomicBoolean();
        var originalPath = sourceFileService.getMappingPath();
        var updatedPath = sourceFileService.getTempMappingPath();
        Connection conn = null;

        // Count the total number of unmapped sources in order to present progress
        int totalUnmapped = calculateTotalUnmapped(originalPath);

        // Simultaneously read from the existing mapping and write to a new temporary mapping
        try (
            var originalParser = SourceFileService.openMappingsParser(originalPath);
            var updatedPrinter = SourceFileService.openMappingsPrinter(updatedPath);
        ) {
            conn = indexService.openDbConnection();
            // Have to make reference to connection final so it can be used inside the download block
            final var dbConn = conn;
            var imageDir = fileRetrievalService.getSshCollectionPath().resolve(CdmFileRetrievalService.IMAGE_SUBPATH);
            var pdfDir = fileRetrievalService.getSshCollectionPath().resolve(CdmFileRetrievalService.PDF_SUBPATH);

            fileRetrievalService.executeDownloadBlock((scpClient -> {
                try {
                    var exportSourceFilesPath = initializeExportSourceFilesDir();

                    int currentUnmapped = 0;
                    for (CSVRecord originalRecord : originalParser) {
                        var origMapping = SourceFileService.recordToMapping(originalRecord);
                        // skip over already populated mappings
                        if (origMapping.getSourcePaths() != null) {
                            SourceFileService.writeMapping(updatedPrinter, origMapping);
                            continue;
                        }

                        currentUnmapped++;
                        // Figure out name of associated file and download it
                        var fileInfo = retrieveSourceFileNameAndEntryTypeField(dbConn, origMapping);
                        var entryTypeField = fileInfo.get(1);
                        String filename;
                        String filePath;
                        // Pdf and image cpd objects are located in different places
                        if (CdmIndexService.ENTRY_TYPE_DOCUMENT_PDF.equals(entryTypeField)) {
                            // add cdmid to filename to prevent overwriting
                            filename = origMapping.getCdmId() + "_index.pdf";
                            filePath = pdfDir.resolve(origMapping.getCdmId() + "/index.pdf").toString();
                        } else {
                            filename = fileInfo.get(0);
                            filePath = imageDir.resolve(filename).toString();
                        }
                        var destPath = exportSourceFilesPath.resolve(filename);

                        try {
                            scpClient.download(filePath, destPath);
                        }  catch (IOException e) {
                            log.warn("Failed to download file {} to {}: {}", filePath, destPath, e.getMessage());
                            failedToDownload.set(true);
                            // Write the original mapping, with no source path set
                            SourceFileService.writeMapping(updatedPrinter, origMapping);
                            continue;
                        }
                        // Update mapping to include downloaded file
                        origMapping.setSourcePaths(destPath.toString());
                        origMapping.setMatchingValue(filename);
                        SourceFileService.writeMapping(updatedPrinter, origMapping);

                        outputLogger.info("Downloaded source file {} for object {} ({} / {})",
                                filename, origMapping.getCdmId(), currentUnmapped, totalUnmapped);
                    }
                } catch (IOException | SQLException e) {
                    throw new MigrationException("Encountered an error while downloading source files", e);
                }
            }));
        } catch (SQLException e) {
            throw new MigrationException("Failed to establish database connection", e);
        } finally {
            CdmIndexService.closeDbConnection(conn);
        }
        // Switch the updated mapping file over to being the primary mapping
        var swapPath = Paths.get(originalPath.toString() + "_old");
        Files.move(originalPath, swapPath);
        Files.move(updatedPath, originalPath);
        Files.delete(swapPath);

        return failedToDownload.get() ? "One or more source files failed to download, check the logs" : null;
    }

    private int calculateTotalUnmapped(Path originalPath) throws IOException {
        int total = 0;
        try (var originalParser = SourceFileService.openMappingsParser(originalPath)) {
            for (CSVRecord originalRecord : originalParser) {
                if (StringUtils.isBlank(originalRecord.get(2))) {
                    total++;
                }
            }
        }
        return total;
    }

    private static final String FILENAME_QUERY =
            "select find, " + CdmIndexService.ENTRY_TYPE_FIELD + " from "
                    + CdmIndexService.TB_NAME + " where " + CdmFieldInfo.CDM_ID + " = ?";

    private List<String> retrieveSourceFileNameAndEntryTypeField(Connection conn, SourceFileMapping mapping)
            throws SQLException {
        try (var filenameStmt = conn.prepareStatement(FILENAME_QUERY)) {
            filenameStmt.setString(1, mapping.getCdmId());
            var resultSet = filenameStmt.executeQuery();
            if (resultSet.next()) {
                String sourceFilename = resultSet.getString(1);
                String entryTypeField = resultSet.getString(2);
                return Arrays.asList(sourceFilename, entryTypeField);
            } else {
                throw new MigrationException("No record found in index for mapped id " + mapping.getCdmId());
            }
        }
    }

    private Path initializeExportSourceFilesDir() throws IOException {
        var path = CdmFileRetrievalService.getExportedSourceFilesPath(project);
        Files.createDirectories(path);
        return path;
    }

    private void validateProjectState() {
        MigrationProjectProperties props = project.getProjectProperties();
        if (props.getSourceFilesUpdatedDate() == null) {
            throw new InvalidProjectStateException("Source files must be mapped");
        }
    }

    public void setFileRetrievalService(CdmFileRetrievalService fileRetrievalService) {
        this.fileRetrievalService = fileRetrievalService;
    }

    public void setProject(MigrationProject project) {
        this.project = project;
    }

    public void setSourceFileService(SourceFileService sourceFileService) {
        this.sourceFileService = sourceFileService;
    }

    public void setIndexService(CdmIndexService indexService) {
        this.indexService = indexService;
    }
}
