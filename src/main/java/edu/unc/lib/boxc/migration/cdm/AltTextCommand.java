package edu.unc.lib.boxc.migration.cdm;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.AltTextOptions;
import edu.unc.lib.boxc.migration.cdm.options.Verbosity;
import edu.unc.lib.boxc.migration.cdm.services.AltTextService;
import edu.unc.lib.boxc.migration.cdm.services.CdmIndexService;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.status.AltTextStatusService;
import edu.unc.lib.boxc.migration.cdm.validators.AltTextValidator;
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
@Command(name = "alt_text",
        description = "Commands related to alt-text")
public class AltTextCommand {
    private static final Logger log = getLogger(AltTextCommand.class);

    @ParentCommand
    private CLIMain parentCommand;

    private MigrationProject project;
    private CdmIndexService indexService;
    private AltTextService altTextService;

    @Command(name = "upload",
            description = {"Upload a CSV to the alt_text folder.",
                    "Alt-text txt files will be created from each row of the CSV, excluding the header row. " +
                    "CSV file must contain a header row and all following rows should have comma-separated" +
                    "dmrecord and alt-text values."})
    public int upload(@Mixin AltTextOptions options) throws Exception {
        validateOptions(options);
        initialize();
        try {
            altTextService.uploadCsv(options);
            return 0;
        } catch (MigrationException | IllegalArgumentException e) {
            outputLogger.info("Cannot upload alt-text file(s): {}", e.getMessage());
            return 1;
        } catch (Exception e) {
            log.error("Failed to upload alt-text file(s)", e);
            outputLogger.info("Failed to upload alt-text file(s): {}", e.getMessage(), e);
            return 1;
        }
    }

    @Command(name="template",
            description = {"Create an alt-text csv template with headers 'dmrecord' and 'alt-text' and " +
                    "the 'dmrecord' column populated with dmrecord values from the collection. " +
                    "'alt-text' values must be manually inputted."})
    public int template(@Mixin AltTextOptions options) throws Exception {
        initialize();
        try {
            altTextService.generateTemplate();
            return 0;
        } catch (MigrationException | IllegalArgumentException e) {
            outputLogger.info("Cannot generate alt-text csv template: {}", e.getMessage());
            return 1;
        } catch (Exception e) {
            log.error("Failed to generate alt-text csv template", e);
            outputLogger.info("Failed to generate alt-text csv template: {}", e.getMessage(), e);
            return 1;
        }
    }

    @Command(name = "validate",
            description = "Validate the alt-text CSV file to upload for this project")
    public int validate(@Mixin AltTextOptions options, @Option(names = { "-f", "--force"},
            description = "Ignore incomplete mappings") boolean force) throws Exception {
        try {
            validateOptions(options);
            initialize();
            AltTextValidator validator = new AltTextValidator();
            validator.setProject(project);
            List<String> errors = validator.validateCsv(options.getAltTextCsvFile(), force);
            if (errors.isEmpty()) {
                outputLogger.info("PASS: Alt-text upload file at path {} is valid",
                        project.getAltTextCsvPath());
                return 0;
            } else {
                if (parentCommand.getVerbosity().equals(Verbosity.QUIET)) {
                    outputLogger.info("FAIL: Alt-text upload file is invalid with {} errors", errors.size());
                } else {
                    outputLogger.info("FAIL: Alt-text upload file at path {} is invalid due to the following issues:",
                            project.getAltTextCsvPath());
                    for (String error : errors) {
                        outputLogger.info("    - " + error);
                    }
                }
                return 1;
            }
        } catch (MigrationException e) {
            log.error("Failed to validate alt-text upload file", e);
            outputLogger.info("FAIL: Failed to validate alt-text upload file: {}", e.getMessage());
            return 1;
        }
    }

    @Command(name = "status",
            description = "Display status of the alt-text file to upload for this project")
    public int status(@Mixin AltTextOptions options) throws Exception {
        try {
            validateOptions(options);
            initialize();
            AltTextStatusService statusService = new AltTextStatusService();
            statusService.setProject(project);
            statusService.report(options.getAltTextCsvFile(), parentCommand.getVerbosity());

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

    private void validateOptions(AltTextOptions options) {
        if (options.getAltTextCsvFile() == null) {
            throw new IllegalArgumentException("Must provide an alt-text CSV path");
        }
    }

    private void initialize() throws IOException {
        Path currentPath = parentCommand.getWorkingDirectory();
        project = MigrationProjectFactory.loadMigrationProject(currentPath);
        indexService = new CdmIndexService();
        indexService.setProject(project);
        altTextService = new AltTextService();
        altTextService.setProject(project);
        altTextService.setIndexService(indexService);
    }
}
