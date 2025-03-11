package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.jobs.VelocicroptorRemoteJob;
import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo;
import edu.unc.lib.boxc.migration.cdm.options.BoxctronFileMappingOptions;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.sql.Connection;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Service for interacting with boxctron access copy files
 * Alternate method for populating access surrogate mappings
 * @author krwong
 */
public class BoxctronFileService extends AccessFileService {
    private static final Logger log = getLogger(BoxctronFileService.class);

    public static final String ORIGINAL_PATH = "original_path";
    public static final String PREDICTED_CLASS = "predicted_class";
    public static final String PREDICTED_CONF = "predicted_conf";
    public static final String BOUNDING_BOX = "bounding_box";
    public static final String EXTENDED_BOX = "extended_box";
    public static final String CORRECTED_CLASS = "corrected_class";
    public static final String[] BOXCTRON_CSV_HEADERS = new String[] {ORIGINAL_PATH, PREDICTED_CLASS,
            PREDICTED_CONF, BOUNDING_BOX, EXTENDED_BOX};
    public static final String[] EXCUSIONS_CSV_HEADERS = new String[] {ORIGINAL_PATH, PREDICTED_CLASS, CORRECTED_CLASS};

    public BoxctronFileService() {}

    /**
     * Generate the boxctron source file mapping
     * @throws IOException
     */
    public void generateMapping(BoxctronFileMappingOptions options) throws IOException {
        assertProjectStateValid();
        ensureMappingState(options);

        // Gather list of all potential source file paths to match against
        Set<String> candidatePaths = gatherCandidatePaths(getVelocicroptorDataPath(project.getProjectPath()));
        // Gather list of all source file paths to exclude
        Set<String> exclusionPaths = Set.of();
        if (options.getExclusionsCsv() != null) {
            exclusionPaths = gatherExclusionPaths(options.getExclusionsCsv());
        }

        Path mappingPath = getMappingPath();
        boolean needsMerge = options.getUpdate() && Files.exists(mappingPath);
        // Write to temp mappings file if doing a dry run, otherwise write to mappings file
        if (needsMerge || options.getDryRun()) {
            mappingPath = getTempMappingPath();
        }
        Files.deleteIfExists(mappingPath);

        SourceFilesInfo sourceFilesInfo = loadMappings(project.getSourceFilesMappingPath());

        // Iterate through source file objects in this collection to match against
        Connection conn = null;
        try (var csvPrinter = openMappingsPrinter(mappingPath)) {
            // cdm ids and original file paths from source file mappings
            for (SourceFilesInfo.SourceFileMapping fileMapping : sourceFilesInfo.getMappings()) {
                String cdmId = fileMapping.getCdmId();
                List<Path> filePaths = fileMapping.getSourcePaths();

                for (Path filePath : filePaths) {
                    if (candidatePaths.contains(filePath.toString()) && !exclusionPaths.contains(filePath.toString())) {
                        log.debug("Found match for {} from field {}", cdmId, filePath);
                        csvPrinter.printRecord(cdmId, filePath.getFileName(),
                                computeAccessPath(filePath), null);
                    } else {
                        csvPrinter.printRecord(cdmId, filePath.getFileName(), null, null);
                    }
                }
            }
        } catch (IOException e) {
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
    private Set<String> gatherCandidatePaths(Path dataPath) throws IOException {
        if (Files.notExists(dataPath)) {
            throw new NoSuchFileException(dataPath + " does not exist");
        }

        Set<String> candidatePaths = new HashSet<>();
        try (Reader reader = Files.newBufferedReader(dataPath);
             CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                .withFirstRecordAsHeader()
                .withHeader(BOXCTRON_CSV_HEADERS)
                .withTrim());
        ) {
            for (CSVRecord csvRecord : csvParser) {
                String originalPath = csvRecord.get(0);
                String predictedClass = csvRecord.get(1);

                if (predictedClass.equals("1")) {
                    candidatePaths.add(originalPath);
                }
            }
        } catch (IOException e) {
            throw new MigrationException("Failed to read boxctron data file", e);
        }
        return candidatePaths;
    }

    /**
     * Read the exclusions csv produced by boxctron and
     * gather original file paths if the corrected_class value is 0 (skip access file)
     * @throws Exception
     */
    private Set<String> gatherExclusionPaths(Path dataPath) throws IOException {
        if (Files.notExists(dataPath)) {
            throw new NoSuchFileException(dataPath + " does not exist");
        }

        Set<String> exclusionPaths = new HashSet<>();
        try (Reader reader = Files.newBufferedReader(dataPath);
             CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                     .withFirstRecordAsHeader()
                     .withHeader(EXCUSIONS_CSV_HEADERS)
                     .withTrim());
        ) {
            for (CSVRecord csvRecord : csvParser) {
                String originalPath = csvRecord.get(0);
                String correctedClass = csvRecord.get(2);

                if (correctedClass.equals("0")) {
                    exclusionPaths.add(originalPath);
                }
            }
        } catch (IOException e) {
            throw new MigrationException("Failed to read exclusion data file", e);
        }
        return exclusionPaths;
    }

    private Path computeAccessPath(Path originalPath) {
        return project.getProjectPath().resolve("processing/results/velocicroptor/output" + originalPath + ".jpg");
    }

    /**
     * @return Path of the velocicroptor data.csv results
     */
    public Path getVelocicroptorDataPath(Path projectPath) {
        return projectPath.resolve(VelocicroptorRemoteJob.VELOCICROPTOR_FILENAME);
    }
}
