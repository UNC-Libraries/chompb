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
import edu.unc.lib.boxc.migration.cdm.services.RedirectMappingIndexService;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

import java.nio.file.Path;
import java.util.concurrent.Callable;

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;

/**
 * @author snluong
 */
@Command(name = "index_redirects",
        description = "Index redirect mappings in DB")
public class IndexRedirectCommand implements Callable<Integer> {
    @ParentCommand
    private CLIMain parentCommand;

    @CommandLine.Option(names = { "--db-connection" },
            description = {"DB connection filepath location. Falls back to REDIRECT_DB_CONNECTION env variable.",
                    "Default: ${DEFAULT-VALUE}"},
            defaultValue = "${env:REDIRECT_DB_CONNECTION}")
    private Path redirectDbConnectionPath;

    private MigrationProject project;
    private RedirectMappingIndexService indexService;

    @Override
    public Integer call() throws Exception {
        try {
            validate();
            Path currentPath = parentCommand.getWorkingDirectory();
            project = MigrationProjectFactory.loadMigrationProject(currentPath);
            indexService = new RedirectMappingIndexService(project);
            indexService.setRedirectDbConnectionPath(redirectDbConnectionPath);
            indexService.init();

            indexService.indexMapping();
            outputLogger.info("Redirect mapping indexing completed. Yay!");
            return 0;
        } catch (MigrationException e) {
            outputLogger.info("Failed to index redirect mapping: {}", e.getMessage());
            return 1;
        }
    }

    private void validate() {
        if (redirectDbConnectionPath == null) {
            throw new MigrationException("The DB connection path must be included");
        }
    }
}
