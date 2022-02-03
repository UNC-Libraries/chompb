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

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.services.ProjectPropertiesService;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

import java.util.List;

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;

/**
 * Command to update migration project properties
 * @author krwong
 */
@Command(name = "config",
        description = "Configure a CDM migration project.")
public class ProjectPropertiesCommand {
    @ParentCommand
    private CLIMain parentCommand;

    private ProjectPropertiesService propertiesService = new ProjectPropertiesService();

    @Command(name = "list",
            description = "List all configurable properties for the project.")
    public int listProperties() throws Exception {
        try {
            MigrationProject project = MigrationProjectFactory
                    .loadMigrationProject(parentCommand.getWorkingDirectory());
            propertiesService.setProject(project);
            propertiesService.getProjectProperties();
            outputLogger.info("Project properties listed.");
            return 0;
        } catch (MigrationException | IllegalArgumentException e) {
            outputLogger.info("{}", e.getMessage());
            return 1;
        } catch (Exception e) {
            outputLogger.info("Failed to list project properties", e.getMessage());
            return 1;
        }
    }

    @Command(name = "set",
            description = "Manually set project properties. You can only set one property at a time. " +
                    "Example: chompb config set collectionNumber 00ddd")
    public int setProperties(@Parameters(index = "0", paramLabel = "<setField>",
                                     description = "Project property to set") String setField,
                             @Parameters(index = "1", paramLabel = "<setValue>",
                                     description = "New project property value") String setValue) throws Exception {
        try {
            MigrationProject project = MigrationProjectFactory
                    .loadMigrationProject(parentCommand.getWorkingDirectory());
            propertiesService.setProject(project);
            propertiesService.setProperties(setField, setValue);
            outputLogger.info("Project property set");
            return 0;
        } catch (MigrationException | IllegalArgumentException e) {
            outputLogger.info("{}", e.getMessage());
            return 1;
        } catch (Exception e) {
            outputLogger.info("Failed to set project property", e.getMessage());
            return 1;
        }
    }
    @Command(name = "unset",
            description = "Manually unset/clear project properties. " +
                    "You can clear multiple properties at the same time. " +
                    "Example: chompb config unset hookId collectionNumber")
    public int unsetProperties(@Parameters(paramLabel = "<unsetField(s)>",
            description = "List of fields to unset") List<String> unsetFields) throws Exception {
        try {
            MigrationProject project = MigrationProjectFactory
                    .loadMigrationProject(parentCommand.getWorkingDirectory());
            propertiesService.setProject(project);
            propertiesService.unsetProperties(unsetFields);
            outputLogger.info("Project property(ies) unset");
            return 0;
        } catch (MigrationException | IllegalArgumentException e) {
            outputLogger.info("{}", e.getMessage());
            return 1;
        } catch (Exception e) {
            outputLogger.info("Failed to unset project property(ies)", e.getMessage());
            return 1;
        }
    }

}