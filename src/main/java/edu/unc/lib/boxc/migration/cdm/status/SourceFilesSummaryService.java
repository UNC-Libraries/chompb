package edu.unc.lib.boxc.migration.cdm.status;

import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo;
import edu.unc.lib.boxc.migration.cdm.options.SourceFileMappingOptions;
import edu.unc.lib.boxc.migration.cdm.options.Verbosity;
import edu.unc.lib.boxc.migration.cdm.services.SourceFileService;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.HashSet;
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

    /**
     * Display summary about source file mapping
     * @param verbosity
     */
    public void summary(SourceFileMappingOptions options, int oldFilesMapped, Verbosity verbosity) {
        int newFilesMapped = newFilesMapped(options);
        int totalFilesMapped = totalFilesMapped(options, newFilesMapped, oldFilesMapped);
        int totalObjects = totalFilesInProject();

        if (verbosity.isNormal()) {
            showField("New Files Mapped", newFilesMapped);
            showField("Total Files Mapped", totalFilesMapped);
            showField("Total Files in Project", totalObjects);
        }
    }

    /**
     * @return number of new files mapped
     */
    public int totalFilesMapped(SourceFileMappingOptions options, int newNumberFilesMapped, int oldNumberFilesMapped) {
        int totalFiles;
        // for dry run, count files in new temp source mapping and existing source mapping
        if (options.getDryRun()) {
            totalFiles = newNumberFilesMapped + oldNumberFilesMapped;
        } else {
            totalFiles = newFilesMapped(options);
        }
        return totalFiles;
    }

    /**
     * @return number of new files mapped
     */
    public int newFilesMapped(SourceFileMappingOptions options) {
        Set<String> indexedIds = getQueryService().getObjectIdSet();
        Set<String> mappedIds = new HashSet<>();
        SourceFileService sourceFileService = getSourceFileService();
        try {
            SourceFilesInfo info = sourceFileService.loadMappings();
            if (options.getDryRun()) {
                info = sourceFileService.loadTempMappings();
            }
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
     * @return old number of files mapped
     */
    public int oldFilesMapped() {
        Set<String> indexedIds = getQueryService().getObjectIdSet();
        Set<String> mappedIds = new HashSet<>();
        SourceFileService sourceFileService = getSourceFileService();
        try {
            SourceFilesInfo info = sourceFileService.loadMappings();
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

    protected SourceFileService getSourceFileService() {
        this.sourceFileService = new SourceFileService();
        sourceFileService.setProject(project);
        return this.sourceFileService;
    }
}
