package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.options.AltTextFileMappingOptions;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URLConnection;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Service for interacting with alt-text files
 * @author krwong
 */
public class AltTextFileService extends SourceFileService {
    private static final Logger log = LoggerFactory.getLogger(AltTextFileService.class);

    private CdmIndexService indexService;
    private List<String> ids;

    public AltTextFileService() {
    }

    /**
     * Add the alt-text file mapping (populates the alt_text mapping with files from the filesystem)
     * @param options
     * @throws Exception
     */
    public void generateAltTextMapping(AltTextFileMappingOptions options) throws Exception {
        assertProjectStateValid();
        ensureMappingState(options);

        // Gather listing of all potential source file paths to match against
        Map<String, String> candidatePaths = gatherAltTextCandidatePaths(options);

        Path mappingPath = getMappingPath();
        boolean needsMerge = options.getUpdate() && Files.exists(mappingPath);
        // Write to temp mappings file if doing a dry run, otherwise write to mappings file
        if (needsMerge || options.getDryRun()) {
            mappingPath = getTempMappingPath();
            ids = null;
        }
        Files.deleteIfExists(mappingPath);

        // Generate alt-text file mapping entry for each file
        try (var csvPrinter = openMappingsPrinter(mappingPath)) {
            for (Map.Entry<String, String> candidate : candidatePaths.entrySet()) {
                if (getIds().contains(candidate.getKey())) {
                    csvPrinter.printRecord(candidate.getKey(), null, candidate.getValue(), null);
                }
            }
        }

        // Performing update operation with existing mapping, need to merge values
        if (needsMerge) {
            mergeUpdates(options, mappingPath);
        }

        if (!options.getDryRun()) {
            setUpdatedDate(Instant.now());
        }
    }

    protected Map<String, String> gatherAltTextCandidatePaths(AltTextFileMappingOptions options)
            throws Exception {
        Path basePath = options.getBasePath();
        if (!Files.isDirectory(basePath)) {
            throw new IllegalArgumentException("Base path must be a directory");
        }

        Map<String, String> candidatePaths = new HashMap<>();
        Files.walkFileTree(basePath, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
                String fileId = path.getFileName().toString().split("_")[0];
                if (!Files.isDirectory(path)
                        && options.getExtensions().contains(FilenameUtils.getExtension(path.toString()).toLowerCase())) {
                    candidatePaths.put(fileId, path.toString());
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc)
                    throws IOException {
                return FileVisitResult.CONTINUE;
            }
        });
        return candidatePaths;
    }

    @Override
    protected void setUpdatedDate(Instant timestamp) throws IOException {
        project.getProjectProperties().setAltTextFilesUpdatedDate(timestamp);
        ProjectPropertiesSerialization.write(project);
    }

    @Override
    public Path getMappingPath() {
        return project.getAltTextFilesMappingPath();
    }

    /**
     * @param path
     * @return Mimetype of the provided file
     * @throws IOException
     */
    public String getMimetype(Path path) throws IOException {
        String mimetype = URLConnection.guessContentTypeFromName(path.getFileName().toString());
        if (mimetype == null || "text/plain".equals(mimetype)) {
            mimetype = Files.probeContentType(path);
        }
        if (mimetype == null) {
            mimetype = "text/plain";
        }
        return mimetype;
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

    private CdmIndexService getIndexService() {
        if (indexService == null) {
            indexService = new CdmIndexService();
            indexService.setProject(project);
        }
        return indexService;
    }
}