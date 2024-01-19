package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo;
import edu.unc.lib.boxc.migration.cdm.options.Verbosity;
import edu.unc.lib.boxc.migration.cdm.status.StatusQueryService;
import org.apache.commons.lang3.StringUtils;
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
public class SourceFilesSummaryService {
    private static final Logger log = getLogger(SourceFilesSummaryService.class);
    protected static final String INDENT = "    ";
    private static final int MIN_LABEL_WIDTH = 20;

    private MigrationProject project;
    private StatusQueryService queryService;

    /**
     * Display summary about source file mapping
     * @param verbosity
     */
    public void summary(int oldNumberFilesMapped, Verbosity verbosity) {
        int newFilesMapped = newFilesMapped(oldNumberFilesMapped);
        int totalFilesMapped = totalFilesMapped();
        int totalObjects = totalFilesInProject();

        if (verbosity.isNormal()) {
            showField("New Files Mapped", newFilesMapped);
        }
        if (verbosity.isNormal()) {
            showField("Total Files Mapped", totalFilesMapped);
        }
        if (verbosity.isNormal()) {
            showField("Total Files in Project", totalObjects);
        }
    }

    /**
     * @return number of new files mapped
     */
    public int newFilesMapped(int oldNumberFilesMapped) {
        int totalFilesMapped = totalFilesMapped();
        return totalFilesMapped - oldNumberFilesMapped;
    }

    /**
     * @return number of files mapped
     */
    public int totalFilesMapped() {
        int totalFilesMapped = 0;

        Set<String> indexedIds = getQueryService().getObjectIdSet();
        Set<String> mappedIds = new HashSet<>();
        SourceFileService sourceFileService = getSourceFileService();
        sourceFileService.setProject(project);
        try {
            SourceFilesInfo info = sourceFileService.loadMappings();
            for (SourceFilesInfo.SourceFileMapping mapping : info.getMappings()) {
                if (mapping.getSourcePaths() != null) {
                    if (indexedIds.contains(mapping.getCdmId())) {
                        mappedIds.add(mapping.getCdmId());
                    }
                }
            }
            totalFilesMapped = mappedIds.size();
            return mappedIds.size();
        } catch (IOException e) {
            log.error("Failed to load mappings", e);
            outputLogger.info("Failed to load mappings: {}", e.getMessage());
        }

        return totalFilesMapped;
    }

    /**
     * @return number of files in project
     */
    public int totalFilesInProject() {
        return getQueryService().countIndexedFileObjects();
    }

    protected void showField(String label, Object value) {
        int padding = MIN_LABEL_WIDTH - label.length();
        outputLogger.info("{}{}: {}{}", INDENT, label, StringUtils.repeat(' ', padding), value);
    }

    public void setProject(MigrationProject project) {
        this.project = project;
    }

    protected SourceFileService getSourceFileService() {
        return new SourceFileService();
    }

    protected StatusQueryService getQueryService() {
        if (queryService == null) {
            return initializeQueryService();
        }
        return queryService;
    }

    public void setQueryService(StatusQueryService queryService) {
        this.queryService = queryService;
    }

    protected StatusQueryService initializeQueryService() {
        this.queryService = new StatusQueryService(project);
        return this.queryService;
    }
}
