package edu.unc.lib.boxc.migration.cdm.status;

import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo;
import edu.unc.lib.boxc.migration.cdm.options.Verbosity;
import edu.unc.lib.boxc.migration.cdm.services.SourceFileService;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Service for source files summary
 * @author krwong
 */
public class SourceFilesSummaryService extends AbstractStatusService {
    private static final Logger log = getLogger(SourceFilesSummaryService.class);

    private SourceFileService sourceFileService;
    private boolean dryRun;
    private int previousStateFilesMapped;
    private List<CSVRecord> previousStatePopulatedFiles;
    private List<CSVRecord> newStatePopulatedFiles;
    private int sampleSize = 20;

    /**
     * Display summary about source file mapping
     * @param verbosity
     */
    public void summary(Verbosity verbosity) {
        int totalFilesMapped = totalFilesMapped();
        int newFilesMapped = totalFilesMapped - previousStateFilesMapped;
        int totalObjects = totalFilesInProject();
        List<CSVRecord> listFiles = listNewFiles();

        if (verbosity.isNormal()) {
            showField("Previous Files Mapped", previousStateFilesMapped);
            showField("New Files Mapped", newFilesMapped);
            showField("Total Files Mapped", totalFilesMapped);
            showField("Total Files in Project", totalObjects);
            showFiles(listFiles);
        }
    }

    /**
     * @return total number of files mapped
     */
    private int totalFilesMapped() {
        return countFilesMapped(getNewStatePopulatedFiles());
    }

    /**
     * @return number of files mapped
     */
    private int countFilesMapped(List<CSVRecord> populatedFiles) {
        Set<String> indexedIds = getQueryService().getObjectIdSet();
        Set<String> mappedIds = new HashSet<>();

        for (CSVRecord file : populatedFiles) {
            if (indexedIds.contains(file.get(SourceFilesInfo.ID_FIELD))) {
                mappedIds.add(file.get(SourceFilesInfo.ID_FIELD));
            }
        }

        return mappedIds.size();
    }

    /**
     * @return number of files in project
     */
    private int totalFilesInProject() {
        return getQueryService().countIndexedFileObjects();
    }

    /**
     * @return load list of all CSV records
     */
    private List<CSVRecord> loadAllFiles(Path mappingPath) {
        List<CSVRecord> allFiles = new ArrayList<>();

        try (var csvParser = SourceFileService.openMappingsParser(mappingPath)) {
            for (CSVRecord csvRecord : csvParser) {
                allFiles.add(csvRecord);
            }
        } catch (IOException e) {
            log.error("Failed to list files", e);
        }
        return allFiles;
    }

    /**
     * @return load list of CSV records with source file populated
     */
    private List<CSVRecord> loadPopulatedFiles(Path mappingPath) {
        List<CSVRecord> populatedFiles = loadAllFiles(mappingPath);

        return populatedFiles.stream().filter(entry -> !entry.get(SourceFilesInfo.SOURCE_FILE_FIELD).isEmpty())
                .collect(Collectors.toList());
    }

    /**
     * @return list of CSV records that have been populated since the previous mapping state
     */
    private List<CSVRecord> listNewlyPopulatedFiles() {
        List<CSVRecord> newMappings = getNewStatePopulatedFiles();
        if (previousStatePopulatedFiles == null) {
            return newMappings;
        }

        Set<String> previousIdentifiers = getIdentifierSet(previousStatePopulatedFiles);
        Set<String> newIdentifiers = getIdentifierSet(newMappings);
        newIdentifiers.removeAll(previousIdentifiers);
        return newMappings.stream().filter(entry -> newIdentifiers.contains(entry.get(SourceFilesInfo.ID_FIELD)))
                .collect(Collectors.toList());
    }

    private Set<String> getIdentifierSet(List<CSVRecord> mappings) {
        return mappings.stream().map(entry -> entry.get(SourceFilesInfo.ID_FIELD)).collect(Collectors.toSet());
    }

    /**
     * @return sample list of new files mapped (every nth file)
     */
    private List<CSVRecord> sampleListNewFiles() {
        List<CSVRecord> completeListNewFiles = listNewlyPopulatedFiles();
        List<CSVRecord> sampleListNewFiles;

        // select every nth file
        int interval = completeListNewFiles.size() / sampleSize;

        sampleListNewFiles = IntStream.range(0, completeListNewFiles.size())
                .filter(n -> n % interval == 0)
                .mapToObj(completeListNewFiles::get)
                .collect(Collectors.toList());

        return sampleListNewFiles;
    }

    /**
     * @return sample list of files mapped (if number of mapped files > sample size) or list of all files mapped
     */
    private List<CSVRecord> listNewFiles() {
        List<CSVRecord> listNewFiles = new ArrayList<>();
        if (Files.exists(getNewMappingPath())) {
            if (getNewStatePopulatedFiles().size() > sampleSize) {
                listNewFiles = sampleListNewFiles();
            } else {
                listNewFiles = listNewlyPopulatedFiles();
            }
        }
        return listNewFiles;
    }

    private Path getNewMappingPath() {
        if (dryRun) {
            return sourceFileService.getTempMappingPath();
        } else {
            return sourceFileService.getMappingPath();
        }
    }

    private Path getPreviousMappingPath() {
        return sourceFileService.getMappingPath();
    }

    public void capturePreviousState() {
        if (Files.exists(getPreviousMappingPath())) {
            setPreviousStateFilesMapped(countFilesMapped(getPreviousStatePopulatedFiles()));
        }
    }

    private void showFiles(List<CSVRecord> listFiles) {
        if (listFiles.isEmpty()) {
            outputLogger.info("Sample unavailable. No new files mapped.");
        } else {
            outputLogger.info("{}{}:", INDENT, "Sample of New Files");
            outputLogger.info("{}{}{}{}{}{}", INDENT, INDENT, SourceFilesInfo.ID_FIELD + ",",
                    SourceFilesInfo.EXPORT_MATCHING_FIELD + ",", SourceFilesInfo.SOURCE_FILE_FIELD + ",",
                    SourceFilesInfo.POTENTIAL_MATCHES_FIELD);
            for (CSVRecord file : listFiles) {
                outputLogger.info("{}{}{}{}{}{}", INDENT, INDENT,
                        file.get(SourceFilesInfo.ID_FIELD) + ",", file.get(SourceFilesInfo.EXPORT_MATCHING_FIELD) + ",",
                        file.get(SourceFilesInfo.SOURCE_FILE_FIELD) + ",",
                        file.get(SourceFilesInfo.POTENTIAL_MATCHES_FIELD));
            }
        }
    }

    public List<CSVRecord> getPreviousStatePopulatedFiles() {
        if (previousStatePopulatedFiles == null) {
            setPreviousStatePopulatedFiles(loadPopulatedFiles(getPreviousMappingPath()));
        }
        return previousStatePopulatedFiles;
    }

    public List<CSVRecord> getNewStatePopulatedFiles() {
        if (newStatePopulatedFiles == null) {
            setNewStatePopulatedFiles(loadPopulatedFiles(getNewMappingPath()));
        }
        return newStatePopulatedFiles;
    }

    public void setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
    }

    public void setPreviousStateFilesMapped(int previousStateFilesMapped) {
        this.previousStateFilesMapped = previousStateFilesMapped;
    }

    public void setPreviousStatePopulatedFiles(List<CSVRecord> previousStatePopulatedFiles) {
        this.previousStatePopulatedFiles = previousStatePopulatedFiles;
    }

    public void setNewStatePopulatedFiles(List<CSVRecord> newStatePopulatedFiles) {
        this.newStatePopulatedFiles = newStatePopulatedFiles;
    }

    public void setSampleSize(int sampleSize) {
        this.sampleSize = sampleSize;
    }

    public void setSourceFileService(SourceFileService sourceFileService) {
        this.sourceFileService = sourceFileService;
    }
}
