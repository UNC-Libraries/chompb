package edu.unc.lib.boxc.migration.cdm.status;

import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo;
import edu.unc.lib.boxc.migration.cdm.options.Verbosity;
import edu.unc.lib.boxc.migration.cdm.services.SourceFileService;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

    /**
     * Display summary about source file mapping
     * @param verbosity
     */
    public void summary(Verbosity verbosity) {
        int previousFilesMapped = previousFilesMapped();
        int totalFilesMapped = totalFilesMapped();
        int newFilesMapped = newFilesMapped(totalFilesMapped, previousFilesMapped);
        int totalObjects = totalFilesInProject();
        List<CSVRecord> listFiles = listNewFiles();

        if (verbosity.isNormal()) {
            showField("Previous Files Mapped", previousFilesMapped);
            showField("New Files Mapped", newFilesMapped);
            showField("Total Files Mapped", totalFilesMapped);
            showField("Total Files in Project", totalObjects);
            showFiles(listFiles);
        }
    }

    /**
     * @return number of new files mapped
     */
    public int newFilesMapped(int totalFilesMapped, int previousFilesMapped) {
        return totalFilesMapped - previousFilesMapped;
    }

    /**
     * @return total number of files mapped
     */
    public int totalFilesMapped() {
        return countFilesMapped(getNewMappingPath());
    }

    /**
     * @return previous number of files mapped
     */
    public int previousFilesMapped() {
        return previousStateFilesMapped;
    }

    /**
     * @return number of files mapped
     */
    public int countFilesMapped(Path mappingPath) {
        Set<String> indexedIds = getQueryService().getObjectIdSet();
        Set<String> mappedIds = new HashSet<>();
        try {
            SourceFilesInfo info = SourceFileService.loadMappings(mappingPath);
            for (SourceFilesInfo.SourceFileMapping mapping : info.getMappings()) {
                if (mapping.getSourcePaths() != null && indexedIds.contains(mapping.getCdmId())) {
                    mappedIds.add(mapping.getCdmId());
                }
            }
        } catch (IOException e) {
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
     * @return complete list of new files mapped
     */
    public List<CSVRecord> completeListNewFiles() {
        List<CSVRecord> completeListNewFiles = new ArrayList<>();

        try (
                Reader reader = Files.newBufferedReader(getNewMappingPath());
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                        .withFirstRecordAsHeader()
                        .withHeader(SourceFilesInfo.CSV_HEADERS)
                        .withTrim());
        ) {
            completeListNewFiles = csvParser.getRecords();
        } catch (IOException e) {
            log.error("Failed to list new files", e);
        }
        return completeListNewFiles;
    }

    /**
     * @return sample list of new files mapped (first and last few rows)
     */
    public List<CSVRecord> sampleListNewFiles() {
        List<CSVRecord> completeListNewFiles = completeListNewFiles();
        List<CSVRecord> sampleListNewFiles;

        // sample the first 10 and last 10 files mapped
        if (completeListNewFiles.size() > 20) {
            sampleListNewFiles = completeListNewFiles.subList(0, 9);
            sampleListNewFiles.addAll(completeListNewFiles.subList(completeListNewFiles.size() - 10, completeListNewFiles.size()));
        } else {
            sampleListNewFiles = completeListNewFiles;
        }

        return sampleListNewFiles;
    }

    public List<CSVRecord> listNewFiles() {
        List<CSVRecord> listNewFiles = new ArrayList<>();
        if (Files.exists(getNewMappingPath())) {
            listNewFiles = sampleListNewFiles();
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
        }
    }

    protected void showFiles(List<CSVRecord> listFiles) {
        if (listFiles.isEmpty()) {
            outputLogger.info("Sample unavailable. No new files mapped.");
        } else {
            outputLogger.info("{}{}:", INDENT, "Sample of New Files");
            for (CSVRecord file : listFiles) {
                outputLogger.info("{}{}* {}", INDENT, INDENT, file);
            }
        }
    }

    public void setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
    }

    public void setPreviousStateFilesMapped(int previousStateFilesMapped) {
        this.previousStateFilesMapped = previousStateFilesMapped;
    }

    public void setSourceFileService(SourceFileService sourceFileService) {
        this.sourceFileService = sourceFileService;
    }
}
