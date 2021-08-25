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
        int totalObjects = getQueryService().countIndexedObjects();
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
