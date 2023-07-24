package edu.unc.lib.boxc.migration.cdm.status;

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import edu.unc.lib.boxc.migration.cdm.model.GroupMappingInfo;
import edu.unc.lib.boxc.migration.cdm.options.Verbosity;
import edu.unc.lib.boxc.migration.cdm.services.GroupMappingService;
import org.slf4j.Logger;

/**
 * Service to display status of object group mapping
 *
 * @author bbpennel
 */
public class GroupMappingStatusService extends AbstractStatusService {
    private static final Logger log = getLogger(GroupMappingStatusService.class);

    /**
     * Display a stand alone report of the source file mapping status
     * @param verbosity
     */
    public void report(Verbosity verbosity) {
        outputLogger.info("Group mappings status for project {}", project.getProjectName());
        reportStats(-1, verbosity);
    }

    /**
     * Display status about group mappings
     * @param totalObjects
     * @param verbosity
     */
    public void reportStats(int totalObjects, Verbosity verbosity) {
        Instant generated = project.getProjectProperties().getGroupMappingsUpdatedDate();
        showField("Last Generated", generated == null ? "Not completed" : generated);
        if (generated == null) {
            return;
        }
        Path mappingsPath = project.getGroupMappingPath();
        Instant modified = null;
        if (Files.exists(mappingsPath)) {
            try {
                modified = Files.getLastModifiedTime(mappingsPath).toInstant();
            } catch (IOException e) {
                log.error("Failed to check mappings", e);
                outputLogger.info("Failed to check mappings: {}", e.getMessage());
            }
        }
        showField("Mappings Modified", modified == null ? "Not present" : modified);
        Instant synced = project.getProjectProperties().getGroupMappingsSyncedDate();
        showField("Last Synced", synced == null ? "Not completed" : synced);

        if (!verbosity.isNormal()) {
            return;
        }
        if (totalObjects == -1) {
            totalObjects = getQueryService().countIndexedObjects();
        }

        GroupMappingService groupService = new GroupMappingService();
        groupService.setProject(project);

        try {
            GroupMappingInfo groupInfo = groupService.loadMappings();
            Map<String, List<String>> mappings = groupInfo.getGroupedMappings();
            int totalGroups = 0;
            int childrenInGroups = 0;
            for (Entry<String, List<String>> entry : mappings.entrySet()) {
                if (entry.getValue().size() > 1) {
                    totalGroups++;
                    childrenInGroups += entry.getValue().size();
                }
            }
            showField("Total Groups", totalGroups);
            showFieldWithPercent("Objects In Groups", childrenInGroups, totalObjects);
        } catch (IOException e) {
            log.error("Failed to load mappings", e);
            outputLogger.info("Failed to load mappings: {}", e.getMessage());
        }
    }
}
