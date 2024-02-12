package edu.unc.lib.boxc.migration.cdm.status;

import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo;
import edu.unc.lib.boxc.migration.cdm.options.Verbosity;
import edu.unc.lib.boxc.migration.cdm.services.SourceFileService;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.Reader;
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
    private static final String ID_FIELD = "id";
    private static final String MATCHING_VALUE_FIELD = "matching_value";
    private static final String SOURCE_FILE_FIELD = "source_file";
    private static final String POTENTIAL_MATCHES_FIELD = "potential_matches";

    private SourceFileService sourceFileService;
    private boolean dryRun;
    private int previousStateFilesMapped;
    private List<CSVRecord> previousStatePopulatedFiles;
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
    public int totalFilesMapped() {
        return countFilesMapped(getNewMappingPath());
    }

    /**
     * @return number of files mapped
     */
    public int countFilesMapped(Path mappingPath) {
        Set<String> indexedIds = getQueryService().getObjectIdSet();
        Set<String> mappedIds = new HashSet<>();
        try {
            List<CSVRecord> populatedEntries = loadPopulatedEntries(mappingPath);
            for (CSVRecord entry : populatedEntries) {
                if (!entry.get(SOURCE_FILE_FIELD).isEmpty() && indexedIds.contains(entry.get(ID_FIELD))) {
                    mappedIds.add(entry.get(ID_FIELD));
                }
            }
        } catch (Exception e) {
            log.error("Failed to load mappings", e);
            outputLogger.info("Failed to load mappings: {}", e.getMessage());
        }
        return mappedIds.size();
    }

    /**
     * @return number of files in project
     */
    public int totalFilesInProject() {
        return getQueryService().countIndexedFileObjects();
    }

    /**
     * @return load list of all CSV records
     */
    public List<CSVRecord> loadAllEntries(Path mappingPath) {
        List<CSVRecord> allEntries = new ArrayList<>();

        try (
                Reader reader = Files.newBufferedReader(mappingPath);
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                        .withFirstRecordAsHeader()
                        .withHeader(SourceFilesInfo.CSV_HEADERS)
                        .withTrim());
        ) {
            for (CSVRecord csvRecord : csvParser) {
                allEntries.add(csvRecord);
            }
        } catch (IOException e) {
            log.error("Failed to list files", e);
        }
        return allEntries;
    }

    /**
     * @return load list of CSV records with source file populated
     */
    public List<CSVRecord> loadPopulatedEntries(Path mappingPath) {
        List<CSVRecord> populatedEntries = loadAllEntries(mappingPath);

        return populatedEntries.stream().filter(entry -> !entry.get(SOURCE_FILE_FIELD).isEmpty())
                .collect(Collectors.toList());
    }

    /**
     * @return list of new CSV records with source file populated
     */
    public List<CSVRecord> getListNewEntries() {
        List<CSVRecord> previousMappings = new ArrayList<>();
        if (previousStatePopulatedFiles != null) {
            previousMappings = previousStatePopulatedFiles;
        }
        List<CSVRecord> newMappings = loadPopulatedEntries(getNewMappingPath());

        if (!previousMappings.isEmpty()) {
            Set<String> previousIdentifiers = getIdentifierSet(previousMappings);
            Set<String> newIdentifiers = getIdentifierSet(newMappings);
            newIdentifiers.removeAll(previousIdentifiers);
            return newMappings.stream().filter(entry -> newIdentifiers.contains(entry.get(ID_FIELD)))
                    .collect(Collectors.toList());
        } else {
            return newMappings;
        }
    }

    private Set<String> getIdentifierSet(List<CSVRecord> mappings) {
        return mappings.stream().map(entry -> entry.get(ID_FIELD)).collect(Collectors.toSet());
    }

    /**
     * @return sample list of new files mapped (every nth file)
     */
    public List<CSVRecord> sampleListNewFiles() {
        List<CSVRecord> completeListNewFiles = getListNewEntries();
        List<CSVRecord> sampleListNewFiles;

        // select every nth entry
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
    public List<CSVRecord> listNewFiles() {
        List<CSVRecord> listNewFiles = new ArrayList<>();
        if (Files.exists(getNewMappingPath())) {
            if (loadPopulatedEntries(getNewMappingPath()).size() > sampleSize) {
                listNewFiles = sampleListNewFiles();
            } else {
                listNewFiles = loadPopulatedEntries(getNewMappingPath());
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
            setPreviousStateFilesMapped(countFilesMapped(getPreviousMappingPath()));
            setPreviousStatePopulatedFiles(loadPopulatedEntries(getPreviousMappingPath()));
        }
    }

    protected void showFiles(List<CSVRecord> listFiles) {
        if (listFiles.isEmpty()) {
            outputLogger.info("Sample unavailable. No new files mapped.");
        } else {
            outputLogger.info("{}{}:", INDENT, "Sample of New Files");
            outputLogger.info("{}{}{}{}{}{}", INDENT, INDENT, ID_FIELD + ",", MATCHING_VALUE_FIELD + ",",
                    SOURCE_FILE_FIELD + ",", POTENTIAL_MATCHES_FIELD);
            for (CSVRecord file : listFiles) {
                outputLogger.info("{}{}{}{}{}{}", INDENT, INDENT,
                        file.get(ID_FIELD) + ",", file.get(MATCHING_VALUE_FIELD) + ",",
                        file.get(SOURCE_FILE_FIELD) + ",", file.get(POTENTIAL_MATCHES_FIELD));
            }
        }
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

    public void setSampleSize(int sampleSize) {
        this.sampleSize = sampleSize;
    }

    public void setSourceFileService(SourceFileService sourceFileService) {
        this.sourceFileService = sourceFileService;
    }
}
