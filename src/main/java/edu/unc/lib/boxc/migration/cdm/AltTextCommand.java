package edu.unc.lib.boxc.migration.cdm;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.Verbosity;
import edu.unc.lib.boxc.migration.cdm.services.AltTextService;
import edu.unc.lib.boxc.migration.cdm.services.CdmIndexService;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.status.AltTextStatusService;
import edu.unc.lib.boxc.migration.cdm.validators.AltTextValidator;
import org.slf4j.Logger;
import picocli.CommandLine.Command;
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
        description = "Commands related to alt-text mappings")
public class AltTextCommand {
    private static final Logger log = getLogger(AltTextCommand.class);

    @ParentCommand
    private CLIMain parentCommand;

    private MigrationProject project;
    private CdmIndexService indexService;
    private AltTextService altTextService;

    @Command(name="generate",
            description = {"Generate the optional alt-text mapping file for this project.",
                    "A blank alt_text_files.csv template will be created for this project, " +
                    "with only cdm dmrecords populated."})
    public int generate() throws Exception {
        long start = System.nanoTime();

        try {
            initialize();
            altTextService.generateAltTextMapping();
            outputLogger.info("Alt-text mapping generated for {} in {}s", project.getProjectName(),
                    (System.nanoTime() - start) / 1e9);
            return 0;
        } catch (MigrationException | IllegalArgumentException e) {
            outputLogger.info("Cannot generate alt-text mapping: {}", e.getMessage());
            return 1;
        } catch (Exception e) {
            log.error("Failed to generate alt-text template", e);
            outputLogger.info("Failed to generate alt-text template: {}", e.getMessage(), e);
            return 1;
        }
    }

    @Command(name = "validate",
            description = "Validate the alt-text mappings for this project")
    public int validate(@Option(names = { "-f", "--force"},
            description = "Ignore incomplete mappings") boolean force) throws Exception {
        try {
            initialize();
            AltTextValidator validator = new AltTextValidator();
            validator.setProject(project);
            List<String> errors = validator.validateMappings(force);
            if (errors.isEmpty()) {
                outputLogger.info("PASS: Alt-text mapping at path {} is valid",
                        project.getAltTextMappingPath());
                return 0;
            } else {
                if (parentCommand.getVerbosity().equals(Verbosity.QUIET)) {
                    outputLogger.info("FAIL: Alt-text mapping is invalid with {} errors", errors.size());
                } else {
                    outputLogger.info("FAIL: Alt-text mapping at path {} is invalid due to the following issues:",
                            project.getAltTextMappingPath());
                    for (String error : errors) {
                        outputLogger.info("    - " + error);
                    }
                }
                return 1;
            }
        } catch (MigrationException e) {
            log.error("Failed to validate alt-text mappings", e);
            outputLogger.info("FAIL: Failed to validate alt-text mappings: {}", e.getMessage());
            return 1;
        }
    }

    @Command(name = "status",
            description = "Display status of the alt-text mappings for this project")
    public int status() throws Exception {
        try {
            initialize();
            AltTextStatusService statusService = new AltTextStatusService();
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
