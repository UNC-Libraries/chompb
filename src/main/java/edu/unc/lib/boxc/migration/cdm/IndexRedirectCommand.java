package edu.unc.lib.boxc.migration.cdm;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.services.RedirectMappingIndexService;
import org.slf4j.Logger;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

import java.nio.file.Path;
import java.util.concurrent.Callable;

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author snluong
 */
@Command(name = "index_redirects",
        description = "Index redirect mappings in DB")
public class IndexRedirectCommand implements Callable<Integer> {
    private static final Logger log = getLogger(IndexRedirectCommand.class);
    @ParentCommand
    private CLIMain parentCommand;

    @CommandLine.Option(names = { "--db-connection" },
            description = {"DB connection filepath location. Falls back to REDIRECT_DB_CONNECTION env variable.",
                    "Default: ${DEFAULT-VALUE}"},
            defaultValue = "${env:REDIRECT_DB_CONNECTION}")
    private Path redirectDbConnectionPath;

    private MigrationProject project;
    private RedirectMappingIndexService indexService;

    @Override
    public Integer call() throws Exception {
        try {
            validate();
            Path currentPath = parentCommand.getWorkingDirectory();
            project = MigrationProjectFactory.loadMigrationProject(currentPath);
            indexService = new RedirectMappingIndexService(project);
            indexService.setRedirectDbConnectionPath(redirectDbConnectionPath);
            indexService.init();

            indexService.indexMapping();
            outputLogger.info("Redirect mapping indexing completed. Yay!");
            return 0;
        } catch (MigrationException e) {
            outputLogger.info("Failed to index redirect mapping: {}", e.getMessage());
            log.error("Failed to index redirect mapping", e);
            return 1;
        }
    }

    private void validate() {
        if (redirectDbConnectionPath == null) {
            throw new MigrationException("The DB connection path must be included");
        }
    }
}
