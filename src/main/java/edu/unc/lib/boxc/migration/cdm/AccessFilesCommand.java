package edu.unc.lib.boxc.migration.cdm;

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import edu.unc.lib.boxc.migration.cdm.status.SourceFilesSummaryService;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.SourceFileMappingOptions;
import edu.unc.lib.boxc.migration.cdm.options.Verbosity;
import edu.unc.lib.boxc.migration.cdm.services.AccessFileService;
import edu.unc.lib.boxc.migration.cdm.services.CdmIndexService;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.status.AccessFilesStatusService;
import edu.unc.lib.boxc.migration.cdm.validators.AccessFilesValidator;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

/**
 * @author bbpennel
 */
@Command(name = "access_files",
        description = "Commands related to access file mappings")
public class AccessFilesCommand {
    private static final Logger log = getLogger(AccessFilesCommand.class);

    @ParentCommand
    private CLIMain parentCommand;

    private MigrationProject project;
    private SourceFilesSummaryService summaryService;
    private AccessFileService accessService;

    @Command(name = "generate",
            description = {
                    "Generate the optional access copy mapping file for this project.",
                    "See the source_files command for more details about usage"})
    public int generate(@Mixin SourceFileMappingOptions options) throws Exception {
        long start = System.nanoTime();

        try {
            validateOptions(options);
            initialize(options.getDryRun());

            accessService.generateMapping(options);
            summaryService.summary(Verbosity.NORMAL);
            outputLogger.info("Access mapping generated for {} in {}s", project.getProjectName(),
                    (System.nanoTime() - start) / 1e9);
            return 0;
        } catch (MigrationException | IllegalArgumentException e) {
            outputLogger.info("Cannot generate access mapping: {}", e.getMessage());
            return 1;
        } catch (Exception e) {
            log.error("Failed to map access files", e);
            outputLogger.info("Failed to map access files: {}", e.getMessage(), e);
            return 1;
        }
    }

    @Command(name = "validate",
            description = "Validate the access file mappings for this project")
    public int validate(@Option(names = { "-f", "--force"},
            description = "Ignore incomplete mappings") boolean force) throws Exception {
        try {
            initialize(false);
            AccessFilesValidator validator = new AccessFilesValidator();
            validator.setProject(project);
            List<String> errors = validator.validateMappings(force);
            if (errors.isEmpty()) {
                outputLogger.info("PASS: Access file mapping at path {} is valid",
                        project.getSourceFilesMappingPath());
                return 0;
            } else {
                if (parentCommand.getVerbosity().equals(Verbosity.QUIET)) {
                    outputLogger.info("FAIL: Access file mapping is invalid with {} errors", errors.size());
                } else {
                    outputLogger.info("FAIL: Access file mapping at path {} is invalid due to the following issues:",
                            project.getSourceFilesMappingPath());
                    for (String error : errors) {
                        outputLogger.info("    - " + error);
                    }
                }
                return 1;
            }
        } catch (MigrationException e) {
            log.error("Failed to validate access file mappings", e);
            outputLogger.info("FAIL: Failed to validate access file mappings: {}", e.getMessage());
            return 1;
        }
    }

    @Command(name = "status",
            description = "Display status of the access file mappings for this project")
    public int status() throws Exception {
        try {
            initialize(false);
            AccessFilesStatusService statusService = new AccessFilesStatusService();
            statusService.setProject(project);
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

    private void validateOptions(SourceFileMappingOptions options) {
        if (options.getBasePath() == null) {
            throw new IllegalArgumentException("Must provide a base path");
        }
        if (StringUtils.isBlank(options.getExportField())) {
            throw new IllegalArgumentException("Must provide an export field");
        }
    }

    private void initialize(boolean dryRun) throws IOException {
        Path currentPath = parentCommand.getWorkingDirectory();
        project = MigrationProjectFactory.loadMigrationProject(currentPath);
        CdmIndexService indexService = new CdmIndexService();
        indexService.setProject(project);
        summaryService = new SourceFilesSummaryService();
        summaryService.setProject(project);
        summaryService.setDryRun(dryRun);
        accessService = new AccessFileService();
        accessService.setIndexService(indexService);
        accessService.setProject(project);
    }

}
