package edu.unc.lib.boxc.migration.cdm;

import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.services.MigrationTypeReportService;
import picocli.CommandLine;

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
            String numWorks = String.valueOf(typeReportService.countWorks());
            String numFiles = String.valueOf(typeReportService.countFiles());
            outputLogger.info("Number of Works: {}", numWorks);
            outputLogger.info("Number of Files: {}", numFiles);
            return 0;
        } catch (Exception e) {
            outputLogger.info("Encountered an error while counting new objects", e);
        }
        return 1;
    }
}
