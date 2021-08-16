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
package edu.unc.lib.boxc.migration.cdm;

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;

import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.CdmFieldService;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

/**
 * @author bbpennel
 */
@Command(name = "fields",
        description = "Interactions with CDM fields for the current project")
public class CdmFieldsCommand {
    @ParentCommand
    private CLIMain parentCommand;

    private CdmFieldService fieldService = new CdmFieldService();

    @Command(name = "validate",
            description = "Validate the cdm_fields.json file for this project")
    public int validateFields() throws Exception {
        try {
            MigrationProject project = MigrationProjectFactory
                    .loadMigrationProject(parentCommand.getWorkingDirectory());
            fieldService.validateFieldsFile(project);
            outputLogger.info("PASS: CDM fields file at path {} is valid", project.getFieldsPath());
            return 0;
        } catch (InvalidProjectStateException e) {
            outputLogger.info("FAIL: {}", e.getMessage());
            return 1;
        } catch (MigrationException e) {
            outputLogger.info("FAIL: Failed to validate fields file: {}", e.getMessage());
            return 1;
        }
    }
}