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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.unc.lib.boxc.migration.cdm.AccessFilesCommand;
import org.apache.commons.lang3.StringUtils;

import edu.unc.lib.boxc.migration.cdm.model.DestinationsInfo;
import edu.unc.lib.boxc.migration.cdm.model.DestinationsInfo.DestinationMapping;
import edu.unc.lib.boxc.migration.cdm.options.Verbosity;
import edu.unc.lib.boxc.migration.cdm.services.DestinationsService;
import edu.unc.lib.boxc.migration.cdm.validators.DestinationsValidator;
import org.slf4j.Logger;

/**
 * Service which reports status of destination mappings
 * @author bbpennel
 */
public class DestinationsStatusService extends AbstractStatusService {
    private static final Logger log = getLogger(DestinationsStatusService.class);
    /**
     * Display a stand alone report of the destination mapping status
     * @param verbosity
     */
    public void report(Verbosity verbosity) {
        outputLogger.info("Destinations status for project {}", project.getProjectName());
        int totalObjects = getQueryService().countIndexedObjects();
        reportDestinationStats(totalObjects, verbosity);
    }

    /**
     * Display status about destinations mappings
     * @param totalObjects
     * @param verbosity
     */
    public void reportDestinationStats(int totalObjects, Verbosity verbosity) {
        Instant destsGenerated = project.getProjectProperties().getDestinationsGeneratedDate();
        showField("Last Generated", destsGenerated == null ? "Not completed" : destsGenerated);
        if (destsGenerated == null) {
            return;
        }
        try {
            Set<String> indexedIds = getQueryService().getObjectIdSet();
            Set<String> unknownIds = new HashSet<>();

            DestinationsValidator validator = new DestinationsValidator();
            validator.setProject(project);
            List<String> errors = validator.validateMappings(false);
            int numErrors = errors.size();
            if (numErrors == 0) {
                showField("Destinations Valid", "Yes");
            } else {
                showField("Destinations Valid", "No (" + numErrors + " errors)");
                if (verbosity.isVerbose()) {
                    showFieldListValues(errors);
                }
                if (verbosity.isNormal()) {
                    outputLogger.info("{}**WARNING: Invalid mappings may impact other details**", INDENT);
                }
            }

            DestinationsInfo destInfo = DestinationsService.loadMappings(project);
            Set<String> dests = new HashSet<>();
            Set<String> newColls = new HashSet<>();
            Set<String> mappedIds = new HashSet<>();
            Set<String> missingDest = new HashSet<>();
            boolean hasDefault = false;
            for (DestinationMapping destMapping : destInfo.getMappings()) {
                String dest = destMapping.getDestination();
                boolean isDefault = DestinationsInfo.DEFAULT_ID.equals(destMapping.getId());
                if (isDefault) {
                    hasDefault = dest != null;
                }
                if (!StringUtils.isBlank(dest)) {
                    dests.add(dest + " " + destMapping.getCollectionId());
                    if (!isDefault) {
                        if (indexedIds.contains(destMapping.getId())) {
                            mappedIds.add(destMapping.getId());
                        } else {
                            unknownIds.add(destMapping.getId());
                        }
                    }
                    if (!StringUtils.isBlank(destMapping.getCollectionId())) {
                        newColls.add(destMapping.getCollectionId());
                    }
                } else {
                    missingDest.add(destMapping.getId());
                }
            }

            int totalMapped = hasDefault ? totalObjects - missingDest.size() : mappedIds.size();
            showFieldWithPercent("Objects Mapped", totalMapped, totalObjects);
            if (verbosity.isNormal()) {
                showFieldWithPercent("Unmapped Objects", totalObjects - totalMapped, totalObjects);
            }
            if (verbosity.isVerbose()) {
                if (hasDefault) {
                    showFieldListValues(missingDest);
                } else {
                    indexedIds.removeAll(mappedIds);
                    showFieldListValues(indexedIds);
                }
            }
            if (verbosity.isNormal()) {
                showField("Unknown Objects", unknownIds.size() + " (Object IDs that are mapped but not indexed)");
            }
            if (verbosity.isVerbose()) {
                showFieldListValues(unknownIds);
            }
            if (verbosity.isNormal()) {
                int toDefault = hasDefault ? totalObjects - mappedIds.size() : 0;
                showFieldWithPercent("To Default", toDefault, totalObjects);
            }
            showField("Destinations", dests.size());
            if (verbosity.isNormal()) {
                showFieldListValues(dests);
            }
            if (verbosity.isNormal()) {
                showField("New Collections", newColls.size());
                showFieldListValues(newColls);
            }
        } catch (IOException e) {
            log.error("Failed to load destinations mapping", e);
            outputLogger.info("Failed to load destinations mapping: {}", e.getMessage());
        }
    }
}
