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

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.GenerateDestinationMappingOptions;
import edu.unc.lib.boxc.migration.cdm.services.DestinationsService;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.ParentCommand;

/**
 * @author bbpennel
 */
@Command(name = "destinations",
        description = "Commands related to destination mappings")
public class DestinationsCommand {
    private static final Logger log = getLogger(DestinationsCommand.class);
    @ParentCommand
    private CLIMain parentCommand;

    private MigrationProject project;
    private DestinationsService destService;

    @Command(name = "generate",
            description = "Generate the destination mapping file for this project")
    public int generate(@Mixin GenerateDestinationMappingOptions options) throws Exception {
        long start = System.nanoTime();

        try {
            validateOptions(options);
            initialize();

            destService.generateMapping(options);
            outputLogger.info("Destination mapping generated for {} in {}s", project.getProjectName(),
                    (System.nanoTime() - start) / 1e9);
            return 0;
        } catch (MigrationException | IllegalArgumentException e) {
            outputLogger.info("Cannot generate mappings: {}", e.getMessage());
            return 1;
        } catch (Exception e) {
            log.error("Failed to export project", e);
            outputLogger.info("Failed to export project: {}", e.getMessage(), e);
            return 1;
        }
    }

    private void validateOptions(GenerateDestinationMappingOptions options) {
        // For now, the only kind of mapping is a default, so fail if not set
        if (StringUtils.isBlank(options.getDefaultDestination())) {
            throw new IllegalArgumentException("Must provide a default destination");
        }
        if (options.getDefaultCollection() != null && StringUtils.isBlank(options.getDefaultCollection())) {
            throw new IllegalArgumentException("Default collection must not be blank");
        }
    }

    private void initialize() throws IOException {
        Path currentPath = parentCommand.getWorkingDirectory();
        project = MigrationProjectFactory.loadMigrationProject(currentPath);
        destService = new DestinationsService();
        destService.setProject(project);
    }
}