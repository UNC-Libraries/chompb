package edu.unc.lib.boxc.migration.cdm;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.BoxctronFileMappingOptions;
import edu.unc.lib.boxc.migration.cdm.options.GenerateSourceFileMappingOptions;
import edu.unc.lib.boxc.migration.cdm.options.Verbosity;
import edu.unc.lib.boxc.migration.cdm.services.AccessFileService;
import edu.unc.lib.boxc.migration.cdm.services.BoxctronFileService;
import edu.unc.lib.boxc.migration.cdm.services.CdmIndexService;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.status.AccessFilesStatusService;
import edu.unc.lib.boxc.migration.cdm.status.SourceFilesSummaryService;
import edu.unc.lib.boxc.migration.cdm.validators.AccessFilesValidator;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author krwong
 */
@Command(name = "boxctron_files",
        description = "Commands related to boxctron access file mappings")
public class BoxctronFileCommand {
    private static final Logger log = getLogger(BoxctronFileCommand.class);

    @ParentCommand
    private CLIMain parentCommand;

    private MigrationProject project;
    private SourceFilesSummaryService summaryService;
    private BoxctronFileService boxctronFileService;

    @Command(name = "generate",
            description = {
                    "Generate the optional boxctron access copy mapping file for this project."})
    public int generate(@Mixin BoxctronFileMappingOptions options) throws Exception {
        long start = System.nanoTime();

        try {
            initialize(options.getDryRun());

            summaryService.capturePreviousState();
            boxctronFileService.generateMapping(options);
            summaryService.summary(parentCommand.getVerbosity());
            outputLogger.info("Boxctron access mapping generated for {} in {}s", project.getProjectName(),
                    (System.nanoTime() - start) / 1e9);
            return 0;
        } catch (MigrationException | IllegalArgumentException e) {
            outputLogger.info("Cannot generate boxctron access mapping: {}", e.getMessage());
            return 1;
        } catch (Exception e) {
            log.error("Failed to map boxctron access files", e);
            outputLogger.info("Failed to map boxctron access files: {}", e.getMessage(), e);
            return 1;
        }
    }

    @Command(name = "validate",
            description = "Validate the boxctron access file mappings for this project")
    public int validate(@Option(names = { "-f", "--force"},
            description = "Ignore incomplete mappings") boolean force) throws Exception {
        try {
            initialize(false);
            AccessFilesValidator validator = new AccessFilesValidator();
            validator.setProject(project);
            List<String> errors = validator.validateMappings(force);
            if (errors.isEmpty()) {
                outputLogger.info("PASS: Boxctron access file mapping at path {} is valid",
                        project.getAccessFilesMappingPath());
                return 0;
            } else {
                if (parentCommand.getVerbosity().equals(Verbosity.QUIET)) {
                    outputLogger.info("FAIL: Boxctron access file mapping is invalid with {} errors", errors.size());
                } else {
                    outputLogger.info("FAIL: Boxctron access file mapping at path {} is invalid due to the following issues:",
                            project.getAccessFilesMappingPath());
                    for (String error : errors) {
                        outputLogger.info("    - " + error);
                    }
                }
                return 1;
            }
        } catch (MigrationException e) {
            log.error("Failed to validate boxctron access file mappings", e);
            outputLogger.info("FAIL: Failed to validate boxctron access file mappings: {}", e.getMessage());
            return 1;
        }
    }

    @Command(name = "status",
            description = "Display status of the boxctron access file mappings for this project")
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

    private void initialize(boolean dryRun) throws IOException {
        Path currentPath = parentCommand.getWorkingDirectory();
        project = MigrationProjectFactory.loadMigrationProject(currentPath);
        CdmIndexService indexService = new CdmIndexService();
        indexService.setProject(project);
        boxctronFileService = new BoxctronFileService();
        boxctronFileService.setIndexService(indexService);
        boxctronFileService.setProject(project);
        summaryService = new SourceFilesSummaryService();
        summaryService.setProject(project);
        summaryService.setDryRun(dryRun);
        summaryService.setSourceFileService(boxctronFileService);
    }
}
