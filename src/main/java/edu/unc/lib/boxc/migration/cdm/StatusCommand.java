package edu.unc.lib.boxc.migration.cdm;

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.Callable;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.CdmFieldService;
import edu.unc.lib.boxc.migration.cdm.services.CdmIndexService;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.services.StreamingMetadataService;
import edu.unc.lib.boxc.migration.cdm.status.ProjectStatusService;
import org.slf4j.Logger;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

/**
 * @author bbpennel
 */
@Command(name = "status",
        description = "Display the status of the current project")
public class StatusCommand implements Callable<Integer>  {
    private static final Logger log = getLogger(StatusCommand.class);
    @ParentCommand
    private CLIMain parentCommand;

    private ProjectStatusService statusService;
    private CdmFieldService fieldService;
    private CdmIndexService indexService;
    private StreamingMetadataService streamingMetadataService;
    private MigrationProject project;

    @Override
    public Integer call() throws Exception {
        try {
            initialize();

            statusService.report();

            return 0;
        } catch (MigrationException e) {
            log.error("Failed to report project status", e);
            outputLogger.info("Failed to report project status: {}", e.getMessage());
            return 1;
        }
    }

    private void initialize() throws IOException {
        Path currentPath = parentCommand.getWorkingDirectory();
        project = MigrationProjectFactory.loadMigrationProject(currentPath);

        fieldService = new CdmFieldService();
        indexService = new CdmIndexService();
        indexService.setProject(project);
        streamingMetadataService = new StreamingMetadataService();
        streamingMetadataService.setProject(project);
        streamingMetadataService.setFieldService(fieldService);
        streamingMetadataService.setIndexService(indexService);
        statusService = new ProjectStatusService();
        statusService.setProject(project);
        statusService.setStreamingMetadataService(streamingMetadataService);
    }
}
