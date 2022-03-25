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
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.time.Instant;
import java.util.Set;

import edu.unc.lib.boxc.migration.cdm.AccessFilesCommand;
import edu.unc.lib.boxc.migration.cdm.options.Verbosity;
import edu.unc.lib.boxc.migration.cdm.services.DescriptionsService;
import org.slf4j.Logger;

/**
 * Service for displaying status related to descriptive records
 *
 * @author bbpennel
 */
public class DescriptionsStatusService extends AbstractStatusService {
    private static final Logger log = getLogger(DescriptionsStatusService.class);
    private DescriptionsService descService;

    /**
     * Display a stand alone report of the descriptions mapping status
     * @param verbosity
     */
    public void report(Verbosity verbosity) {
        outputLogger.info("Descriptions status for project {}", project.getProjectName());
        int totalObjects = getQueryService().countIndexedObjects();
        reportStats(totalObjects, verbosity);
    }

    /**
     * Display stats about descriptions
     * @param totalObjects
     * @param verbosity
     */
    public void reportStats(int totalObjects, Verbosity verbosity) {
        showField("MODS Files", countXmlDocuments(project.getDescriptionsPath()));
        showField("New Collections MODS", countXmlDocuments(project.getNewCollectionDescriptionsPath()));
        Instant expanded = project.getProjectProperties().getDescriptionsExpandedDate();
        showField("Last Expanded", expanded == null ? "Not completed" : expanded);
        try {
            Set<String> idsWithMods = descService.expandDescriptions(true);
            showFieldWithPercent("Object MODS Records", idsWithMods.size(), totalObjects);
            if (verbosity.isVerbose()) {
                showField("Objects without MODS", totalObjects - idsWithMods.size());
                Set<String> indexedIds = getQueryService().getObjectIdSet();
                indexedIds.removeAll(idsWithMods);
                showFieldListValues(indexedIds);
            }
        } catch (IOException e) {
            log.error("Failed to list MODS records", e);
            outputLogger.info("Failed to list MODS records: {}", e.getMessage());
        }
    }

    public void setDescriptionsService(DescriptionsService descService) {
        this.descService = descService;
    }
}
