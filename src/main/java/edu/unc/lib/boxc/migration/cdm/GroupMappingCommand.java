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
import edu.unc.lib.boxc.migration.cdm.options.GroupMappingOptions;
import edu.unc.lib.boxc.migration.cdm.services.CdmFieldService;
import edu.unc.lib.boxc.migration.cdm.services.CdmIndexService;
import edu.unc.lib.boxc.migration.cdm.services.GroupMappingService;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.ParentCommand;

/**
 * @author bbpennel
 */
@Command(name = "group_mapping",
        description = "Commands related to grouping objects into works")
public class GroupMappingCommand {
    private static final Logger log = getLogger(GroupMappingCommand.class);

    @ParentCommand
    private CLIMain parentCommand;

    private MigrationProject project;
    private GroupMappingService groupService;

    @Command(name = "generate",
            description = {
                    "Generate the group mapping file for this project.",
                    "The resulting will be written to the group_mappings.csv for this project, unless "
                    + "the --dry-run flag is provided."})
    public int generate(@Mixin GroupMappingOptions options) throws Exception {
        long start = System.nanoTime();

        try {
            validateOptions(options);
            initialize();

            groupService.generateMapping(options);
            outputLogger.info("Group mapping generated for {} in {}s", project.getProjectName(),
                    (System.nanoTime() - start) / 1e9);
            return 0;
        } catch (MigrationException | IllegalArgumentException e) {
            outputLogger.info("Cannot generate group mapping: {}", e.getMessage());
            return 1;
        } catch (Exception e) {
            log.error("Failed to map group objects", e);
            outputLogger.info("Failed to map group objects: {}", e.getMessage(), e);
            return 1;
        }
    }

    @Command(name = "sync",
            description = { "Sync the group mapping file for this project into the index database." } )
    public int sync() throws Exception {
        long start = System.nanoTime();

        try {
            initialize();

            groupService.syncMappings();
            outputLogger.info("Group mapping synched to index for {} in {}s", project.getProjectName(),
                    (System.nanoTime() - start) / 1e9);
            return 0;
        } catch (MigrationException | IllegalArgumentException e) {
            outputLogger.info("Cannot sync group mappings: {}", e.getMessage());
            return 1;
        } catch (Exception e) {
            log.error("Failed to sync group mappings", e);
            outputLogger.info("Failed to sync group mappings: {}", e.getMessage(), e);
            return 1;
        }
    }

    private void initialize() throws IOException {
        Path currentPath = parentCommand.getWorkingDirectory();
        project = MigrationProjectFactory.loadMigrationProject(currentPath);
        CdmIndexService indexService = new CdmIndexService();
        indexService.setProject(project);
        CdmFieldService fieldService = new CdmFieldService();
        groupService = new GroupMappingService();
        groupService.setIndexService(indexService);
        groupService.setProject(project);
        groupService.setFieldService(fieldService);
    }

    private void validateOptions(GroupMappingOptions options) {
        if (StringUtils.isBlank(options.getGroupField())) {
            throw new IllegalArgumentException("Must provide an group field name");
        }
    }
}