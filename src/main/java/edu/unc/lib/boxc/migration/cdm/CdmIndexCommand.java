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
import java.util.concurrent.Callable;

import org.slf4j.Logger;

import edu.unc.lib.boxc.migration.cdm.exceptions.StateAlreadyExistsException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.CdmFieldService;
import edu.unc.lib.boxc.migration.cdm.services.CdmIndexService;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

/**
 * @author bbpennel
 */
@Command(name = "index",
        description = "Index the exported CDM records for this project. Must be run after a complete export.")
public class CdmIndexCommand implements Callable<Integer> {
    private static final Logger log = getLogger(CdmIndexCommand.class);
    @ParentCommand
    private CLIMain parentCommand;

    @Option(names = { "-f", "--force"},
            description = "Overwrite index if one already exists")
    private boolean force;

    private CdmFieldService fieldService;
    private CdmIndexService indexService;
    private MigrationProject project;

    @Override
    public Integer call() throws Exception {
        long start = System.nanoTime();

        try {
            initialize();

            indexService.createDatabase(force);
            indexService.indexAll();
            outputLogger.info("Indexed project {} in {}s", project.getProjectName(),
                    (System.nanoTime() - start) / 1e9);
            return 0;
        } catch (StateAlreadyExistsException e) {
            outputLogger.info("Cannot index project: {}", e.getMessage());
            return 1;
        } catch (Exception e) {
            log.error("Failed to export project", e);
            outputLogger.info("Failed to export project: {}", e.getMessage(), e);
            indexService.removeIndex();
            return 1;
        }
    }

    private void initialize() throws IOException {
        Path currentPath = parentCommand.getWorkingDirectory();
        project = MigrationProjectFactory.loadMigrationProject(currentPath);
        fieldService = new CdmFieldService();
        indexService = new CdmIndexService();
        indexService.setFieldService(fieldService);
        indexService.setProject(project);
    }
}
