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

import org.slf4j.Logger;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.DescriptionsService;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import picocli.CommandLine.Command;
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

            int extracted = descService.expandDescriptions();
            outputLogger.info("Descriptions expanded to {} separate files for {} in {}s",
                    extracted, project.getProjectName(), (System.nanoTime() - start) / 1e9);
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

    private void initialize() throws IOException {
        Path currentPath = parentCommand.getWorkingDirectory();
        project = MigrationProjectFactory.loadMigrationProject(currentPath);
        descService = new DescriptionsService();
        descService.setProject(project);
    }
}
