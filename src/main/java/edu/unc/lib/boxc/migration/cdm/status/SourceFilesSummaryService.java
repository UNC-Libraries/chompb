package edu.unc.lib.boxc.migration.cdm.status;

import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo;
import edu.unc.lib.boxc.migration.cdm.options.Verbosity;
import edu.unc.lib.boxc.migration.cdm.services.SourceFileService;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.Path;
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
    private boolean dryRun;

    /**
     * Display summary about source file mapping
     * @param verbosity
     */
    public void summary(Verbosity verbosity) {
        int oldFilesMapped = oldFilesMapped();
        int newFilesMapped = newFilesMapped();
        int totalFilesMapped = totalFilesMapped(newFilesMapped, oldFilesMapped);
        int totalObjects = totalFilesInProject();

        if (verbosity.isNormal()) {
            showField("New Files Mapped", newFilesMapped);
            showField("Total Files Mapped", totalFilesMapped);
            showField("Total Files in Project", totalObjects);
        }
    }

    /**
     * @return total number of files mapped
     */
    public int totalFilesMapped(int newFilesMapped, int oldFilesMapped) {
        return newFilesMapped + oldFilesMapped;
    }

    /**
     * @return number of new files mapped
     */
    public int newFilesMapped() {
        return countFilesMapped(getNewMappingPath());
    }

    /**
     * @return old number of files mapped
     */
    public int oldFilesMapped() {
        if (!dryRun) {
            return 0;
        } else {
            return countFilesMapped(getOldMappingPath());
        }
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

    private Path getNewMappingPath() {
        if (dryRun) {
            return getSourceFileService().getTempMappingPath();
        } else {
            return project.getSourceFilesMappingPath();
        }
    }

    private Path getOldMappingPath() {
        return project.getSourceFilesMappingPath();
    }

    public void setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
    }

    protected SourceFileService getSourceFileService() {
        this.sourceFileService = new SourceFileService();
        sourceFileService.setProject(project);
        return this.sourceFileService;
    }
}
