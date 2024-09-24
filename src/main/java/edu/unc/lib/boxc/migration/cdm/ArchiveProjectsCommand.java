package edu.unc.lib.boxc.migration.cdm;

import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.services.ArchiveProjectsService;
import org.slf4j.Logger;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Command;

import java.util.List;
import java.util.concurrent.Callable;

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;
import static org.slf4j.LoggerFactory.getLogger;

@Command(name = "archive",
        description = {"Archive a project or list of projects. These projects will be moved to an 'archived' folder."})
public class ArchiveProjectsCommand implements Callable<Integer> {
    private static final Logger log = getLogger(ArchiveProjectsCommand.class);

    @ParentCommand
    private CLIMain parentCommand;

    @Option(names = { "-p", "--project-names" },
            split = ",",
            description = {"Specify project or list of projects to be archived"})
    private List<String> projectNames;

    private ArchiveProjectsService archiveProjectsService;

    @Override
    public Integer call() throws Exception {
        try {
            archiveProjectsService = new ArchiveProjectsService();
            archiveProjectsService.archiveProjects(parentCommand.getWorkingDirectory(), projectNames);
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
