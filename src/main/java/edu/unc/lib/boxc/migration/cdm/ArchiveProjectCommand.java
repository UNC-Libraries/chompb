package edu.unc.lib.boxc.migration.cdm;

import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.services.ArchiveProjectService;
import org.slf4j.Logger;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Command;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;
import static org.slf4j.LoggerFactory.getLogger;

@Command(name = "archive",
        description = {"Archive a project or list of projects. These projects will be moved to an 'archived' folder."})
public class ArchiveProjectCommand implements Callable<Integer> {
    private static final Logger log = getLogger(ArchiveProjectCommand.class);

    @ParentCommand
    private CLIMain parentCommand;

    @Option(names = { "-p", "--project-paths" },
            split = ",",
            description = {"Specify project or list of projects to be archived"})
    private List<Path> projectPaths;

    private ArchiveProjectService archiveProjectService;

    @Override
    public Integer call() throws Exception {
        try {
            archiveProjectService = new ArchiveProjectService();
            archiveProjectService.archiveProject(parentCommand.getWorkingDirectory(), projectPaths);
            return 0;
        } catch(InvalidProjectStateException e) {
            outputLogger.info("Archiving project(s) failed: {}", e.getMessage());
            return 1;
        } catch (Exception e) {
            log.error("Failed to archive project(s)", e);
            outputLogger.info("Archiving project(s) failed: {}", e.getMessage());
            return 1;
        }
    }
}
