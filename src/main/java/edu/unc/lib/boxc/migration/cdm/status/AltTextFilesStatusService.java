package edu.unc.lib.boxc.migration.cdm.status;

import edu.unc.lib.boxc.migration.cdm.options.Verbosity;
import edu.unc.lib.boxc.migration.cdm.services.AltTextFileService;
import edu.unc.lib.boxc.migration.cdm.services.SourceFileService;
import edu.unc.lib.boxc.migration.cdm.validators.AltTextFilesValidator;
import edu.unc.lib.boxc.migration.cdm.validators.SourceFilesValidator;

import java.time.Instant;

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;

/**
 * Service for displaying reports of alt-text file mappings
 * @author krwong
 */
public class AltTextFilesStatusService extends SourceFilesStatusService {
    /**
     * Display a stand alone report of the source file mapping status
     * @param verbosity
     */
    @Override
    public void report(Verbosity verbosity) {
        outputLogger.info("Alt-text file mappings status for project {}", project.getProjectName());
        int totalObjects = getQueryService().countIndexedFileObjects();
        reportStats(totalObjects, verbosity);
    }

    @Override
    protected Instant getUpdatedDate() {
        return project.getProjectProperties().getAltTextFilesUpdatedDate();
    }

    @Override
    protected SourceFilesValidator getValidator() {
        return new AltTextFilesValidator();
    }

    @Override
    protected SourceFileService getMappingService() {
        return new AltTextFileService();
    }

    @Override
    protected boolean forceValidation() {
        return true;
    }
}
