package edu.unc.lib.boxc.migration.cdm;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.Verbosity;
import edu.unc.lib.boxc.migration.cdm.services.AspaceRefIdService;
import edu.unc.lib.boxc.migration.cdm.services.CdmFieldService;
import edu.unc.lib.boxc.migration.cdm.services.CdmIndexService;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.validators.AspaceRefIdValidator;
import org.slf4j.Logger;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author krwong
 */
@Command(name = "aspace_ref_id",
        description = "Commands related to aspace ref id mappings")
public class AspaceRefIdCommand {
    private static final Logger log = getLogger(AltTextCommand.class);

    @ParentCommand
    private CLIMain parentCommand;

    private MigrationProject project;
    private CdmFieldService fieldService;
    private CdmIndexService indexService;
    private AspaceRefIdService aspaceRefIdService;

    @Command(name = "generate",
            description = {"Generate the optional aspace ref id mapping file for this project.",
                    "A blank ref_id_mapping.csv template will be created for this project, " +
                            "with only record ids populated."})
    public int generate() throws Exception {
        long start = System.nanoTime();

        try {
            initialize();
            aspaceRefIdService.generateBlankAspaceRefIdMapping();
            outputLogger.info("Aspace ref id mapping generated for {} in {}s", project.getProjectName(),
                    (System.nanoTime() - start) / 1e9);
            return 0;
        } catch (MigrationException | IllegalArgumentException e) {
            outputLogger.info("Cannot generate aspace ref id mapping: {}", e.getMessage());
            return 1;
        } catch (Exception e) {
            log.error("Failed to generate aspace ref id template", e);
            outputLogger.info("Failed to generate aspace ref id template: {}", e.getMessage(), e);
            return 1;
        }
    }

    @Command(name = "generate_from_csv",
            description = {"Generate the optional aspace ref id mapping file for this project " +
                    "using hookid_to_refid_map.csv.",
                    "A ref_id_mapping.csv template will be created for this project, " +
                            "with record ids and aspace ref ids populated."})
    public int generateFromCsv() throws Exception {
        long start = System.nanoTime();

        try {
            initialize();
            aspaceRefIdService.generateAspaceRefIdMappingFromHookIdRefIdCsv();
            outputLogger.info("Aspace ref id mapping generated for {} in {}s", project.getProjectName(),
                    (System.nanoTime() - start) / 1e9);
            return 0;
        } catch (MigrationException | IllegalArgumentException e) {
            outputLogger.info("Cannot generate aspace ref id mapping: {}", e.getMessage());
            return 1;
        } catch (Exception e) {
            log.error("Failed to generate aspace ref id template", e);
            outputLogger.info("Failed to generate aspace ref id template: {}", e.getMessage(), e);
            return 1;
        }
    }

    @Command(name = "validate",
            description = {"Validate the aspace ref id mappings for this project"})
    public int validate(@Option(names = { "-f", "--force"},
            description = "Ignore incomplete mappings") boolean force) throws Exception {
        try {
            initialize();
            AspaceRefIdValidator validator = new AspaceRefIdValidator();
            validator.setProject(project);
            List<String> errors = validator.validateMappings(force);
            if (errors.isEmpty()) {
                outputLogger.info("PASS: Aspace ref id mapping at path {} is valid",
                        project.getAspaceRefIdMappingPath());
                return 0;
            } else {
                if (parentCommand.getVerbosity().equals(Verbosity.QUIET)) {
                    outputLogger.info("FAIL: Aspace ref id mapping is invalid with {} errors", errors.size());
                } else {
                    outputLogger.info("FAIL: Aspace ref id mapping at path {} is invalid due to the following issues:",
                            project.getAspaceRefIdMappingPath());
                    for (String error : errors) {
                        outputLogger.info("    - " + error);
                    }
                }
                return 1;
            }
        } catch (MigrationException e) {
            log.error("Failed to validate aspace ref id mappings", e);
            outputLogger.info("FAIL: Failed to validate aspace ref id mappings: {}", e.getMessage());
            return 1;
        }
    }

    private void initialize() throws IOException {
        Path currentPath = parentCommand.getWorkingDirectory();
        project = MigrationProjectFactory.loadMigrationProject(currentPath);
        fieldService = new CdmFieldService();
        fieldService.setProject(project);
        indexService = new CdmIndexService();
        indexService.setProject(project);
        aspaceRefIdService = new AspaceRefIdService();
        aspaceRefIdService.setProject(project);
        aspaceRefIdService.setFieldService(fieldService);
        aspaceRefIdService.setIndexService(indexService);
        aspaceRefIdService.setHookIdRefIdMapPath(parentCommand.getChompbConfig().getBxcEnvironments()
                .get(project.getProjectProperties().getBxcEnvironmentId()).getHookIdRefIdMapPath());
    }
}
