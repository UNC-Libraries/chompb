/**
 * Copyright 2008 The University of North Carolina at Chapel Hill
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.unc.lib.boxc.migration.cdm.status;

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import edu.unc.lib.boxc.migration.cdm.model.GroupMappingInfo;
import edu.unc.lib.boxc.migration.cdm.options.Verbosity;
import edu.unc.lib.boxc.migration.cdm.services.GroupMappingService;

/**
 * Service to display status of object group mapping
 *
 * @author bbpennel
 */
public class GroupMappingStatusService extends AbstractStatusService {

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
                outputLogger.info("Failed to check mappings: {}", e.getMessage());
            }
        }
        showField("Mappings Modified", modified == null ? "Not present" : modified);
        Instant synched = project.getProjectProperties().getGroupMappingsSynchedDate();
        showField("Last Synched", synched == null ? "Not completed" : synched);

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
            List<String> groupList = new ArrayList<>();
            for (Entry<String, List<String>> entry : mappings.entrySet()) {
                if (entry.getValue().size() > 1) {
                    totalGroups++;
                    childrenInGroups += entry.getValue().size();
                    if (verbosity.isVerbose()) {
                        String matched = groupInfo.getMatchedValueByGroupKey(entry.getKey());
                        groupList.add(matched + " (with id " + entry.getKey() + "): " + entry.getValue().size());
                    }
                }
            }
            showField("Total Groups", totalGroups);
            showFieldWithPercent("Objects In Groups", childrenInGroups, totalObjects);
            if (verbosity.isVerbose()) {
                showField("Counts per group", "");
                showFieldListValues(groupList);
            }
        } catch (IOException e) {
            outputLogger.info("Failed to load mappings: {}", e.getMessage());
        }
    }
}
