package edu.unc.lib.boxc.migration.cdm;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.AspaceRefIdService;
import edu.unc.lib.boxc.migration.cdm.services.CdmIndexService;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import org.slf4j.Logger;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

import java.io.IOException;
import java.nio.file.Path;

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
    private CdmIndexService indexService;
    private AspaceRefIdService aspaceRefIdService;

    @Command(name="generate",
            description = {"Generate the optional aspace ref id mapping file for this project.",
                    "A blank ref_id_mapping.csv template will be created for this project, " +
                            "with only cdm dmrecords populated."})
    public int generate() throws Exception {
        long start = System.nanoTime();

        try {
            initialize();
            aspaceRefIdService.generateAspaceRefIdMapping();
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

    private void initialize() throws IOException {
        Path currentPath = parentCommand.getWorkingDirectory();
        project = MigrationProjectFactory.loadMigrationProject(currentPath);
        indexService = new CdmIndexService();
        indexService.setProject(project);
        aspaceRefIdService = new AspaceRefIdService();
        aspaceRefIdService.setProject(project);
        aspaceRefIdService.setIndexService(indexService);
    }
}
