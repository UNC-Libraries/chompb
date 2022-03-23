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
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.Verbosity;
import edu.unc.lib.boxc.migration.cdm.services.DescriptionsService;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.status.DescriptionsStatusService;
import edu.unc.lib.boxc.migration.cdm.validators.DescriptionsValidator;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

/**
 * @author bbpennel
 */
@Command(name = "descriptions",
    description = "Commands description files")
public class DescriptionsCommand {
    private static final Logger log = getLogger(DescriptionsCommand.class);

    @ParentCommand
    private CLIMain parentCommand;

    private MigrationProject project;
    private DescriptionsService descService;

    @Command(name = "expand",
            description = { "Expand user provided modsCollection files into separate XML files.",
                "The XML files produced will be named based on the CDM ID value found in each document."})
    public int expand() throws Exception {
        long start = System.nanoTime();

        try {
            initialize();

            Set<String> idsWithMods = descService.expandDescriptions();
            outputLogger.info("Descriptions expanded to {} separate files for {} in {}s",
                    idsWithMods.size(), project.getProjectName(), (System.nanoTime() - start) / 1e9);
            return 0;
        } catch (MigrationException | IllegalArgumentException e) {
            outputLogger.info("Cannot expand descriptions: {}", e.getMessage());
            return 1;
        } catch (Exception e) {
            log.error("Failed to expand descriptions for project", e);
            outputLogger.info("Failed to expand descriptions for project: {}", e.getMessage(), e);
            return 1;
        }
    }

    @Command(name = "generate",
            description = "Generate dummy MODS records with CDM ID fields in a modsCollection wrapper"
                    + " for all the objects in this migration." )
    public int generate(@Option(names = { "-f", "--force"},
            description = "Overwrite generated descriptions file ") boolean force) throws Exception {
        long start = System.nanoTime();

        try {
            initialize();

            int generated = descService.generateDocuments(force);
            outputLogger.info("Description file generated at: {}", descService.getGeneratedModsPath());
            outputLogger.info("Generated {} dummy descriptions for {} in {}s",
                    generated, project.getProjectName(), (System.nanoTime() - start) / 1e9);
            return 0;
        } catch (MigrationException e) {
            log.error("Cannot generate descriptions", e);
            outputLogger.info("Cannot generate descriptions: {}", e.getMessage());
            return 1;
        } catch (Exception e) {
            log.error("Failed to generate descriptions for project", e);
            outputLogger.info("Failed to generate descriptions for project: {}", e.getMessage(), e);
            return 1;
        }
    }

    @Command(name = "validate",
            description = { "Validate the descriptions for this project.",
                    "Files will be validated against the MODS schema, schematron, and checked for root elements"})
    public int validate() throws Exception {
        try {
            initialize();
            DescriptionsValidator validator = new DescriptionsValidator();
            validator.setProject(project);
            validator.init();
            List<String> errors = validator.validate();
            if (errors.isEmpty()) {
                outputLogger.info("PASS: All description files are valid",
                        project.getDestinationMappingsPath());
                return 0;
            } else {
                if (parentCommand.getVerbosity().equals(Verbosity.QUIET)) {
                    outputLogger.info("FAIL: Description files are invalid with {} errors", errors.size());
                } else {
                    outputLogger.info("FAIL: Description files are invalid due to the following {} errors:",
                            errors.size());
                    for (String error : errors) {
                        outputLogger.info("    - " + error);
                    }
                }
                return 1;
            }
        } catch (MigrationException e) {
            log.error("Failed to validate descriptions", e);
            outputLogger.info("FAIL: Failed to validate descriptions: {}", e.getMessage());
            return 1;
        }
    }

    @Command(name = "status",
            description = "Display status of descriptions for this project")
    public int status() throws Exception {
        try {
            initialize();
            DescriptionsStatusService statusService = new DescriptionsStatusService();
            statusService.setProject(project);
            statusService.setDescriptionsService(descService);
            statusService.report(parentCommand.getVerbosity());

            return 0;
        } catch (MigrationException | IllegalArgumentException e) {
            outputLogger.info("Status failed: {}", e.getMessage());
            return 1;
        } catch (Exception e) {
            log.error("Status failed", e);
            outputLogger.info("Status failed: {}", e.getMessage(), e);
            return 1;
        }
    }

    private void initialize() throws IOException {
        Path currentPath = parentCommand.getWorkingDirectory();
        project = MigrationProjectFactory.loadMigrationProject(currentPath);
        descService = new DescriptionsService();
        descService.setProject(project);
    }
}
