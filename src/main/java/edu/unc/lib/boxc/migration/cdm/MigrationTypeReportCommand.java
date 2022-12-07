package edu.unc.lib.boxc.migration.cdm;

import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.services.MigrationTypeReportService;
import picocli.CommandLine;

import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.concurrent.Callable;

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;

/**
 * @author krwong
 */
@CommandLine.Command(name = "report_migration_types",
        description = "Counts new migrated works and files")
public class MigrationTypeReportCommand implements Callable<Integer> {
    @CommandLine.ParentCommand
    private CLIMain parentCommand;
    private MigrationTypeReportService typeReportService;
    private MigrationProject project;


    public void init() throws Exception {
        Path currentPath = parentCommand.getWorkingDirectory();
        project = MigrationProjectFactory.loadMigrationProject(currentPath);
        typeReportService = new MigrationTypeReportService();
        typeReportService.setProject(project);
    }

    @Override
    public Integer call() throws Exception {
        try {
            long start = System.nanoTime();
            init();
            outputLogger.info("Number of Works: {}", typeReportService.countWorks());
            outputLogger.info("Number of Files: {}", typeReportService.countFiles());
            return 0;
        } catch (NoSuchFileException e) {
            outputLogger.info("Cannot generate migration types report. Post migration report not found: {}",
                    e.getMessage());
        } catch (Exception e) {
            outputLogger.info("Encountered an error while counting new objects: {}", e.getMessage());
        }
        return 1;
    }
}
