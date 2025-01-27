package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo;
import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo.SourceFileMapping;
import edu.unc.lib.boxc.migration.cdm.options.BoxctronFileMappingOptions;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FilenameUtils;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Service for interacting with boxctron access copy files
 * Alternate method for popupating access surrogate mappings
 * @author krwong
 */
public class BoxctronFileService extends AccessFileService {
    public static final String ORIGINAL_PATH = "original_path";
    public static final String PREDICTED_CLASS = "predicted_class";
    public static final String PREDICTED_CONF = "predicted_conf";
    public static final String BOUNDING_BOX = "bounding_box";
    public static final String EXTENDED_BOX = "extended_box";
    public static final String[] DATA_CSV_HEADERS = new String[] { ORIGINAL_PATH, PREDICTED_CLASS, PREDICTED_CONF,
            BOUNDING_BOX, EXTENDED_BOX };

    public BoxctronFileService() {}

    /**
     * Generate the boxctron source file mapping
     * @throws IOException
     */
    public void generateMapping(BoxctronFileMappingOptions options) throws IOException {
        assertProjectStateValid();

        // Gather listing of all potential source file paths to match against
        Map<String, String> candidatePaths = gatherCandidatePaths(project.getBoxctronDataPath());

        Path mappingPath = getMappingPath();
        boolean needsMerge = options.getUpdate() && Files.exists(mappingPath);
        // Write to temp mappings file if doing a dry run, otherwise write to mappings file
        if (needsMerge || options.getDryRun()) {
            mappingPath = getTempMappingPath();
        }
        Files.deleteIfExists(mappingPath);

        // Iterate through exported objects in this collection to match against
        Connection conn = null;
        try (var csvPrinter = openMappingsPrinter(mappingPath)) {
            String query = "select " + CdmFieldInfo.CDM_ID + ", file"
                    + " from " + CdmIndexService.TB_NAME;

            conn = indexService.openDbConnection();
            Statement stmt = conn.createStatement();
            var rs = stmt.executeQuery(query);
            while (rs.next()) {
                if (!rs.getString(1).isEmpty() && !rs.getString(2).isEmpty()) {
                    String cdmId = rs.getString(1);
                    String filename = rs.getString(2);

                    if (candidatePaths.containsKey(filename)) {
                        String candidatePath = candidatePaths.get(filename);
                        log.debug("Found match for {} from field {}", cdmId, candidatePath);
                        csvPrinter.printRecord(cdmId, filename,
                                computeAccessPath(candidatePath), null);
                    } else {
                        csvPrinter.printRecord(cdmId, filename, null, null);
                    }
                }
            }
        } catch (SQLException e) {
            throw new MigrationException("Error interacting with export index", e);
        } finally {
            CdmIndexService.closeDbConnection(conn);
        }

        // Performing update operation with existing mapping, need to merge values
        if (needsMerge) {
            mergeUpdates(options, mappingPath);
        }

        if (!options.getDryRun()) {
            setUpdatedDate(Instant.now());
        }
    }

    /**
     * Read the data.csv produced by boxctron and
     * gather original file paths if the predicted_class value is 1 (color bar detected)
     * @throws Exception
     */
    private Map<String, String> gatherCandidatePaths(Path dataPath) throws IOException {
        if (Files.notExists(dataPath)) {
            throw new NoSuchFileException(dataPath + " does not exist");
        }

        Map<String, String> candidatePathsMap = new HashMap<>();
        try (Reader reader = Files.newBufferedReader(dataPath);
             CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                .withFirstRecordAsHeader()
                .withHeader(DATA_CSV_HEADERS)
                .withTrim());
        ) {
            for (CSVRecord csvRecord : csvParser) {
                String originalPath = csvRecord.get(0);
                String predictedClass = csvRecord.get(1);

                if (predictedClass.contains("1")) {
                    candidatePathsMap.put(FilenameUtils.getName(originalPath), originalPath);
                }
            }
        } catch (IOException e) {
            throw new MigrationException("Failed to read boxctron data file", e);
        }
        return candidatePathsMap;
    }

    private Path computeAccessPath(String originalPath) {
        return project.getProjectPath().resolve("processing/results/velocicroptor/output" + originalPath + ".jpg");
    }

    /**
     * Merge existing mappings with updated mappings, writing to temporary files as intermediates
     * @param options
     * @param updatesPath the temp path containing the newly generated mappings to merge into the original mappings
     */
    private void mergeUpdates(BoxctronFileMappingOptions options, Path updatesPath) throws IOException {
        Path originalPath = getMappingPath();
        Path mergedPath = originalPath.getParent().resolve("~" + originalPath.getFileName().toString() + "_merged");
        // Cleanup temp merged path if it already exists
        Files.deleteIfExists(mergedPath);

        // Load the new mappings into memory
        SourceFilesInfo updateInfo = loadMappings(updatesPath);

        // Iterate through the existing mappings, merging in the updated mappings when appropriate
        try (
                var originalParser = openMappingsParser(originalPath);
                // Write to temp mappings file if doing a dry run, otherwise write to mappings file
                var mergedPrinter = openMappingsPrinter(mergedPath)
        ) {
            Set<String> origIds = new HashSet<>();
            for (CSVRecord originalRecord : originalParser) {
                var origMapping = recordToMapping(originalRecord);

                SourceFileMapping updateMapping = updateInfo.getMappingByCdmId(origMapping.getCdmId());
                if (updateMapping == null) {
                    // No updates, so write original
                    writeMapping(mergedPrinter, origMapping);
                } else if (updateMapping.getSourcePaths() != null) {
                    var resolvedMapping = resolveSourcePathConflict(options, origMapping, updateMapping);
                    writeMapping(mergedPrinter, resolvedMapping);
                } else if (updateMapping.getPotentialMatches() != null) {
                    if (origMapping.getSourcePaths() != null) {
                        // Prefer existing match, write original
                        writeMapping(mergedPrinter, origMapping);
                    } else {
                        // merge potential matches
                        if (origMapping.getPotentialMatches() != null) {
                            Set<String> merged = Stream.concat(
                                            origMapping.getPotentialMatches().stream(),
                                            updateMapping.getPotentialMatches().stream())
                                    .collect(Collectors.toSet());
                            updateMapping.setPotentialMatches(new ArrayList<>(merged));
                        }
                        // Write entry with updated potential matches
                        writeMapping(mergedPrinter, updateMapping);
                    }
                } else {
                    // No change, retain original
                    writeMapping(mergedPrinter, origMapping);
                }
                origIds.add(originalRecord.get(0));
            }

            // Add in any records that were updated but not present in the original document
            Set<String> updateIds = updateInfo.getMappings().stream()
                    .map(SourceFileMapping::getCdmId).collect(Collectors.toSet());
            updateIds.removeAll(origIds);
            for (String id : updateIds) {
                writeMapping(mergedPrinter, updateInfo.getMappingByCdmId(id));
            }
        }

        // swap the merged mappings to be the main mappings, unless we're doing a dry run
        if (options.getDryRun()) {
            Files.move(mergedPath, updatesPath, StandardCopyOption.REPLACE_EXISTING);
        } else {
            Files.move(mergedPath, originalPath, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    protected SourceFileMapping resolveSourcePathConflict(BoxctronFileMappingOptions options,
                                                          SourceFileMapping origMapping,
                                                          SourceFileMapping updateMapping) {
        if (options.isForce() || origMapping.getSourcePaths() == null) {
            // overwrite entry with updated mapping source path if using force or original didn't have match
            return updateMapping;
        } else {
            // retain original source path
            return origMapping;
        }
    }
}
