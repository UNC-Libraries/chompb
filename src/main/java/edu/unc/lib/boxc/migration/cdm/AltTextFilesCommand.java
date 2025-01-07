package edu.unc.lib.boxc.migration.cdm;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.AltTextFileMappingOptions;
import edu.unc.lib.boxc.migration.cdm.options.Verbosity;
import edu.unc.lib.boxc.migration.cdm.services.AltTextFileService;
import edu.unc.lib.boxc.migration.cdm.services.CdmIndexService;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.status.AltTextFilesStatusService;
import edu.unc.lib.boxc.migration.cdm.status.SourceFilesSummaryService;
import edu.unc.lib.boxc.migration.cdm.validators.AltTextFilesValidator;
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
@Command(name = "alt_text_files",
        description = "Commands related to alt-text file mappings")
public class AltTextFilesCommand {
    private static final Logger log = getLogger(AltTextCommand.class);

    @ParentCommand
    private CLIMain parentCommand;

    private MigrationProject project;
    private CdmIndexService indexService;
    private SourceFilesSummaryService summaryService;
    private AltTextFileService altTextFileService;

    @Command(name="generate",
            description = {"Generate the optional alt-text mapping file for this project.",
                    "Mappings are produced by listing files from a directory using the --base-path option, "
                    + "then searching for matches between the dmrecord in the filenames and the dmrecord field"
                    + " in the exported CDM records.",
                    "The resulting will be written to the alt_text_files.csv for this project, unless "
                    + "the --dry-run flag is provided."})
    public int generate(@Mixin AltTextFileMappingOptions options) throws Exception {
        long start = System.nanoTime();

        try {
            validateOptions(options);
            initialize(options.getDryRun());

            summaryService.capturePreviousState();
            altTextFileService.generateAltTextMapping(options);
            summaryService.summary(parentCommand.getVerbosity());
            outputLogger.info("Alt-text mapping generated for {} in {}s", project.getProjectName(),
                    (System.nanoTime() - start) / 1e9);
            return 0;
        } catch (MigrationException | IllegalArgumentException e) {
            outputLogger.info("Cannot generate alt-text mapping: {}", e.getMessage());
            return 1;
        } catch (Exception e) {
            log.error("Failed to map alt-text files", e);
            outputLogger.info("Failed to map alt-text files: {}", e.getMessage(), e);
            return 1;
        }
    }

    @Command(name = "validate",
            description = "Validate the alt-text file mappings for this project")
    public int validate(@Option(names = { "-f", "--force"},
            description = "Ignore incomplete mappings") boolean force) throws Exception {
        try {
            initialize(false);
            AltTextFilesValidator validator = new AltTextFilesValidator();
            validator.setProject(project);
            List<String> errors = validator.validateMappings(force);
            if (errors.isEmpty()) {
                outputLogger.info("PASS: Alt-text file mapping at path {} is valid",
                        project.getSourceFilesMappingPath());
                return 0;
            } else {
                if (parentCommand.getVerbosity().equals(Verbosity.QUIET)) {
                    outputLogger.info("FAIL: Alt-text file mapping is invalid with {} errors", errors.size());
                } else {
                    outputLogger.info("FAIL: Alt-text file mapping at path {} is invalid due to the following issues:",
                            project.getSourceFilesMappingPath());
                    for (String error : errors) {
                        outputLogger.info("    - " + error);
                    }
                }
                return 1;
            }
        } catch (MigrationException e) {
            log.error("Failed to validate alt-text file mappings", e);
            outputLogger.info("FAIL: Failed to validate alt-text file mappings: {}", e.getMessage());
            return 1;
        }
    }

    @Command(name = "status",
            description = "Display status of the alt-text file mappings for this project")
    public int status() throws Exception {
        try {
            initialize(false);
            AltTextFilesStatusService statusService = new AltTextFilesStatusService();
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

    private void validateOptions(AltTextFileMappingOptions options) {
        if (options.getBasePath() == null) {
            throw new IllegalArgumentException("Must provide a base path");
        }
    }

    private void initialize(boolean dryRun) throws IOException {
        Path currentPath = parentCommand.getWorkingDirectory();
        project = MigrationProjectFactory.loadMigrationProject(currentPath);
        indexService = new CdmIndexService();
        indexService.setProject(project);
        altTextFileService = new AltTextFileService();
        altTextFileService.setProject(project);
        altTextFileService.setIndexService(indexService);
        summaryService = new SourceFilesSummaryService();
        summaryService.setProject(project);
        summaryService.setDryRun(dryRun);
        summaryService.setSourceFileService(altTextFileService);
    }
}
