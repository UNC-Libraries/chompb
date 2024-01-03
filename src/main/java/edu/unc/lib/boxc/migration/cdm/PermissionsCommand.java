package edu.unc.lib.boxc.migration.cdm;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.PermissionMappingOptions;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.services.PermissionsService;
import org.slf4j.Logger;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Command;

import java.io.IOException;
import java.nio.file.Path;

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

            permissionsService.generateDefaultPermissions(options);
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

    private void initialize() throws IOException {
        Path currentPath = parentCommand.getWorkingDirectory();
        project = MigrationProjectFactory.loadMigrationProject(currentPath);
        permissionsService = new PermissionsService();
        permissionsService.setProject(project);
    }
}
