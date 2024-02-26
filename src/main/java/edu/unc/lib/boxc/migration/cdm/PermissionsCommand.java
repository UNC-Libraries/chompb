package edu.unc.lib.boxc.migration.cdm;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.PermissionMappingOptions;
import edu.unc.lib.boxc.migration.cdm.options.Verbosity;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.services.PermissionsService;
import edu.unc.lib.boxc.migration.cdm.validators.PermissionsValidator;
import org.slf4j.Logger;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Command;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author krwong
 */
@Command(name = "permissions",
        description = "Commands related to permissions mappings")
public class PermissionsCommand {
    private static final Logger log = getLogger(PermissionsCommand.class);
    @ParentCommand
    private CLIMain parentCommand;

    private MigrationProject project;
    private PermissionsService permissionsService;

    @Command(name = "generate",
            description = "Generate the permissions mapping file for this project")
    public int generate(@Mixin PermissionMappingOptions options) throws Exception {
        long start = System.nanoTime();
        try {
            initialize();

            permissionsService.generatePermissions(options);
            outputLogger.info("Permissions mapping generated for {} in {}s", project.getProjectName(),
                    (System.nanoTime() - start) / 1e9);
            return 0;
        } catch (MigrationException | IllegalArgumentException e) {
            outputLogger.info("Cannot generate mappings: {}", e.getMessage());
            return 1;
        } catch (Exception e) {
            log.error("Failed to generate mappings", e);
            outputLogger.info("Failed to generate mappings: {}", e.getMessage(), e);
            return 1;
        }
    }

    @Command(name = "set",
            description = "Add or update an entry in an existing permissions mapping file")
    public int set(@Mixin PermissionMappingOptions options) throws Exception {
        try {
            initialize();

            permissionsService.capturePreviousState();
            permissionsService.setPermissions(options);
            outputLogger.info("Permissions mapping generated for cdmId {}", options.getCdmId());
            return 0;
        } catch (MigrationException | IllegalArgumentException e) {
            outputLogger.info("Cannot set mappings: {}", e.getMessage());
            return 1;
        } catch (Exception e) {
            log.error("Failed to set mappings", e);
            outputLogger.info("Failed to set mappings: {}", e.getMessage(), e);
            return 1;
        }
    }

    @Command(name = "validate",
            description = "Validate the permissions mapping file for this project")
    public int validate() throws Exception {
        try {
            initialize();
            PermissionsValidator validator = new PermissionsValidator();
            validator.setProject(project);
            List<String> errors = validator.validateMappings();
            if (errors.isEmpty()) {
                outputLogger.info("PASS: Permissions mapping at path {} is valid",
                        project.getPermissionsPath());
                return 0;
            } else {
                if (parentCommand.getVerbosity().equals(Verbosity.QUIET)) {
                    outputLogger.info("FAIL: Permissions mapping is invalid with {} errors", errors.size());
                } else {
                    outputLogger.info("FAIL: Permissions mapping at path {} is invalid due to the following issues:",
                            project.getPermissionsPath());
                    for (String error : errors) {
                        outputLogger.info("    - " + error);
                    }
                }
                return 1;
            }
        } catch (MigrationException e) {
            log.error("Failed to validate permissions mappings", e);
            outputLogger.info("FAIL: Failed to validate permissions mappings: {}", e.getMessage());
            return 1;
        }
    }

    private void initialize() throws IOException {
        Path currentPath = parentCommand.getWorkingDirectory();
        project = MigrationProjectFactory.loadMigrationProject(currentPath);
        permissionsService = new PermissionsService();
        permissionsService.setProject(project);
    }
}
