package edu.unc.lib.boxc.migration.cdm.status;

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;

import java.time.Instant;

import edu.unc.lib.boxc.migration.cdm.options.Verbosity;
import edu.unc.lib.boxc.migration.cdm.services.AccessFileService;
import edu.unc.lib.boxc.migration.cdm.services.SourceFileService;
import edu.unc.lib.boxc.migration.cdm.validators.AccessFilesValidator;
import edu.unc.lib.boxc.migration.cdm.validators.SourceFilesValidator;

/**
 * Service for displaying reports of access file mappings
 *
 * @author bbpennel
 */
public class AccessFilesStatusService extends SourceFilesStatusService {

    /**
     * Display a stand alone report of the source file mapping status
     * @param verbosity
     */
    @Override
    public void report(Verbosity verbosity) {
        outputLogger.info("Access file mappings status for project {}", project.getProjectName());
        int totalObjects = getQueryService().countIndexedFileObjects();
        reportStats(totalObjects, verbosity);
    }

    @Override
    protected Instant getUpdatedDate() {
        return project.getProjectProperties().getAccessFilesUpdatedDate();
    }

    @Override
    protected SourceFilesValidator getValidator() {
        return new AccessFilesValidator();
    }

    @Override
    protected SourceFileService getMappingService() {
        return new AccessFileService();
    }

    @Override
    protected boolean forceValidation() {
        return true;
    }
}
