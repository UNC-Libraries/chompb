package edu.unc.lib.boxc.migration.cdm.status;

import edu.unc.lib.boxc.migration.cdm.options.Verbosity;
import edu.unc.lib.boxc.migration.cdm.services.AltTextService;
import edu.unc.lib.boxc.migration.cdm.validators.AltTextValidator;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Service for displaying reports of alt-text upload file
 * @author krwong
 */
public class AltTextStatusService extends AbstractStatusService {
    private static final Logger log = getLogger(AltTextStatusService.class);

    /**
     * Display a stand alone report of the alt-text upload file status
     * @param csvPath
     * @param verbosity
     */
    public void report(Path csvPath, Verbosity verbosity) throws IOException {
        outputLogger.info("Alt-text file status for project {}", project.getProjectName());
        int totalObjects = getQueryService().countIndexedFileObjects();
        reportStats(totalObjects, verbosity, csvPath);
    }

    /**
     * Display status about alt-text upload file
     * @param totalObjects
     * @param verbosity
     * @param csvPath
     */
    public void reportStats(int totalObjects, Verbosity verbosity, Path csvPath) throws IOException {
        Instant altTextUpdated = Files.getLastModifiedTime(csvPath).toInstant();
        showField("Last Updated", altTextUpdated == null ? "Not completed" : altTextUpdated);
        if (altTextUpdated == null) {
            return;
        }
        AltTextValidator validator = getValidator();
        validator.setProject(project);
        List<String> errors = validator.validateCsv(csvPath, forceValidation());
        int numErrors = errors.size();
        if (numErrors == 0) {
            showField("Alt-text Upload File Valid", "Yes");
        } else {
            showField("Alt-text Upload File Valid", "No (" + numErrors + " errors)");
            if (verbosity.isVerbose()) {
                showFieldListValues(errors);
            }
            if (verbosity.isNormal()) {
                outputLogger.info("{}**WARNING: Invalid alt-text values may impact other details**", INDENT);
            }
        }

        Set<String> indexedIds = getQueryService().getObjectIdSet();
        Set<String> altTextIds = new HashSet<>();
        Set<String> unknownIds = new HashSet<>();
        AltTextService altTextService = getAltTextService();
        altTextService.setProject(project);
        try {
            var altTextRecords = altTextService.loadCsvRecords(csvPath);
            for (Map.Entry<String, String> altTextRecord : altTextRecords.entrySet()) {
                if (altTextRecord.getValue() != null) {
                    if (indexedIds.contains(altTextRecord.getKey())) {
                        altTextIds.add(altTextRecord.getKey());
                    } else {
                        unknownIds.add(altTextRecord.getKey());
                    }
                }
            }
            showFieldWithPercent("Objects with Alt-text", altTextIds.size(), totalObjects);
            if (verbosity.isNormal()) {
                showFieldWithPercent("Objects without Alt-text", totalObjects - altTextIds.size(), totalObjects);
            }
            if (verbosity.isVerbose()) {
                indexedIds.removeAll(altTextIds);
                showFieldListValues(indexedIds);
            }
            if (verbosity.isNormal()) {
                showField("Unknown Objects", unknownIds.size() + " (Object IDs that have alt-text but not indexed)");
            }
            if (verbosity.isVerbose()) {
                showFieldListValues(unknownIds);
            }
        } catch (IOException e) {
            log.error("Failed to load alt-text CSV records", e);
            outputLogger.info("Failed to load alt-text CSV records: {}", e.getMessage());
        }
    }

    protected AltTextValidator getValidator() {
        return new AltTextValidator();
    }

    protected AltTextService getAltTextService() {
        return new AltTextService();
    }

    protected boolean forceValidation() {
        return false;
    }
}
