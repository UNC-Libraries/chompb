package edu.unc.lib.boxc.migration.cdm.status;

import edu.unc.lib.boxc.migration.cdm.model.AltTextInfo;
import edu.unc.lib.boxc.migration.cdm.options.Verbosity;
import edu.unc.lib.boxc.migration.cdm.services.AltTextService;
import edu.unc.lib.boxc.migration.cdm.services.CdmIndexService;
import edu.unc.lib.boxc.migration.cdm.validators.AltTextValidator;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Service for displaying reports of alt-text mappings
 * @author krwong
 */
public class AltTextStatusService extends AbstractStatusService {
    private static final Logger log = getLogger(AltTextStatusService.class);

    private CdmIndexService indexService;
    private AltTextService altTextService;

    /**
     * Display a stand alone report of the alt-text mapping status
     * @param verbosity
     */
    public void report(Verbosity verbosity) {
        outputLogger.info("Alt-text mappings status for project {}", project.getProjectName());
        int totalObjects = getQueryService().countIndexedFileObjects();
        reportStats(totalObjects, verbosity);
    }

    public void reportStats(int totalObjects, Verbosity verbosity) {
        Instant altTextUpdated = getUpdatedDate();
        showField("Last Updated", altTextUpdated == null ? "Not completed" : altTextUpdated);
        if (altTextUpdated == null) {
            return;
        }

        AltTextValidator validator = getValidator();
        validator.setProject(project);
        List<String> errors = validator.validateMappings(forceValidation());
        int numErrors = errors.size();
        if (numErrors == 0) {
            showField("Mappings Valid", "Yes");
        } else {
            showField("Mappings Valid", "No (" + numErrors + " errors)");
            if (verbosity.isVerbose()) {
                showFieldListValues(errors);
            }
            if (verbosity.isNormal()) {
                outputLogger.info("{}**WARNING: Invalid mappings may impact other details**", INDENT);
            }
        }

        Set<String> indexedIds = getQueryService().getObjectIdSet();
        Set<String> mappedIds = new HashSet<>();
        Set<String> unknownIds = new HashSet<>();
        getMappingService();

        try {
            AltTextInfo info = altTextService.loadMappings();
            for (AltTextInfo.AltTextMapping mapping : info.getMappings()) {
                if (!StringUtils.isBlank(mapping.getAltTextBody())) {
                    if (indexedIds.contains(mapping.getCdmId())) {
                        mappedIds.add(mapping.getCdmId());
                    } else {
                        unknownIds.add(mapping.getCdmId());
                    }
                }
            }
            showFieldWithPercent("Objects Mapped", mappedIds.size(), totalObjects);
            if (verbosity.isNormal()) {
                showFieldWithPercent("Unmapped Objects", totalObjects - mappedIds.size(), totalObjects);
            }
            if (verbosity.isVerbose()) {
                indexedIds.removeAll(mappedIds);
                showFieldListValues(indexedIds);
            }
            if (verbosity.isNormal()) {
                showField("Unknown Objects", unknownIds.size() + " (Object IDs that are mapped but not indexed)");
            }
            if (verbosity.isVerbose()) {
                showFieldListValues(unknownIds);
            }
        } catch (IOException e) {
            log.error("Failed to load mappings", e);
            outputLogger.info("Failed to load mappings: {}", e.getMessage());
        }
    }

    protected Instant getUpdatedDate() {
        return project.getProjectProperties().getAltTextFilesUpdatedDate();
    }

    protected AltTextValidator getValidator() {
        return new AltTextValidator();
    }

    protected AltTextService getMappingService() {
        if (altTextService == null) {
            altTextService = new AltTextService();
            altTextService.setProject(project);
            altTextService.setIndexService(indexService);
        }
        return altTextService;
    }

    protected boolean forceValidation() {
        return true;
    }

    public void setIndexService(CdmIndexService indexService) {
        this.indexService = indexService;
    }
}
