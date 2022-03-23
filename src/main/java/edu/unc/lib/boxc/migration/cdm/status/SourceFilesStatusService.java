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
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo;
import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo.SourceFileMapping;
import edu.unc.lib.boxc.migration.cdm.options.Verbosity;
import edu.unc.lib.boxc.migration.cdm.services.SourceFileService;
import edu.unc.lib.boxc.migration.cdm.validators.SourceFilesValidator;

/**
 * Service for displaying reports of source file mappings
 *
 * @author bbpennel
 */
public class SourceFilesStatusService extends AbstractStatusService {
    /**
     * Display a stand alone report of the source file mapping status
     * @param verbosity
     */
    public void report(Verbosity verbosity) {
        outputLogger.info("Source file mappings status for project {}", project.getProjectName());
        int totalObjects = getQueryService().countIndexedObjects();
        reportStats(totalObjects, verbosity);
    }

    /**
     * Display status about source file mappings
     * @param totalObjects
     * @param verbosity
     */
    public void reportStats(int totalObjects, Verbosity verbosity) {
        Instant sourceUpdated = getUpdatedDate();
        showField("Last Updated", sourceUpdated == null ? "Not completed" : sourceUpdated);
        if (sourceUpdated == null) {
            return;
        }
        SourceFilesValidator validator = getValidator();
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
        int cntPotential = 0;
        SourceFileService fileService = getMappingService();
        fileService.setProject(project);
        try {
            SourceFilesInfo info = fileService.loadMappings();
            for (SourceFileMapping mapping : info.getMappings()) {
                if (mapping.getSourcePath() != null) {
                    if (indexedIds.contains(mapping.getCdmId())) {
                        mappedIds.add(mapping.getCdmId());
                    } else {
                        unknownIds.add(mapping.getCdmId());
                    }
                } else if (!StringUtils.isBlank(mapping.getPotentialMatchesString())) {
                    cntPotential++;
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
            if (verbosity.isNormal()) {
                showField("Potential Matches", cntPotential);
            }
        } catch (IOException e) {
            outputLogger.info("Failed to load mappings: {}", e);
        }
    }

    protected Instant getUpdatedDate() {
        return project.getProjectProperties().getSourceFilesUpdatedDate();
    }

    protected SourceFilesValidator getValidator() {
        return new SourceFilesValidator();
    }

    protected SourceFileService getMappingService() {
        return new SourceFileService();
    }

    protected boolean forceValidation() {
        return false;
    }
}
