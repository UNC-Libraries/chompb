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
import edu.unc.lib.boxc.migration.cdm.options.SourceFileMappingOptions;
import edu.unc.lib.boxc.migration.cdm.services.CdmIndexService;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.services.SourceFileService;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.ParentCommand;

/**
 * @author bbpennel
 */
@Command(name = "source_files",
        description = "Commands related to source file mappings")
public class SourceFilesCommand {
    private static final Logger log = getLogger(SourceFilesCommand.class);

    @ParentCommand
    private CLIMain parentCommand;

    private MigrationProject project;
    private SourceFileService sourceService;

    @Command(name = "generate",
            description = {
                    "Generate the source mapping file for this project.",
                    "Mappings are produced by listing files from a directory using the --base-path option, "
                    + "then searching for matches between those filenames and some filename field in the "
                    + "exported CDM records.",
                    "The filename field is set using the --field-name option.",
                    "If the value of the filename field does not match the name of the source file, the filename "
                    + " can be transformed using regular expressions via the --field-pattern"
                    + " and --field-pattern options.",
                    "The resulting will be written to the source_files.csv for this project, unless "
                    + "the --dry-run flag is provided."})
    public int generate(@Mixin SourceFileMappingOptions options) throws Exception {
        long start = System.nanoTime();

        try {
            validateOptions(options);
            initialize();

            sourceService.generateMapping(options);
            outputLogger.info("Source file mapping generated for {} in {}s", project.getProjectName(),
                    (System.nanoTime() - start) / 1e9);
            return 0;
        } catch (MigrationException | IllegalArgumentException e) {
            outputLogger.info("Cannot generate source mapping: {}", e.getMessage());
            return 1;
        } catch (Exception e) {
            log.error("Failed to map source files", e);
            outputLogger.info("Failed to map source files: {}", e.getMessage(), e);
            return 1;
        }
    }

    private void validateOptions(SourceFileMappingOptions options) {
        if (options.getBasePath() == null) {
            throw new IllegalArgumentException("Must provide a base path");
        }
        if (StringUtils.isBlank(options.getExportField())) {
            throw new IllegalArgumentException("Must provide an export field");
        }
    }

    private void initialize() throws IOException {
        Path currentPath = parentCommand.getWorkingDirectory();
        project = MigrationProjectFactory.loadMigrationProject(currentPath);
        CdmIndexService indexService = new CdmIndexService();
        indexService.setProject(project);
        sourceService = new SourceFileService();
        sourceService.setIndexService(indexService);
        sourceService.setProject(project);
    }
}
