package edu.unc.lib.boxc.migration.cdm;

import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.ExportObjectsService;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import org.slf4j.Logger;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

import java.nio.file.Path;
import java.util.concurrent.Callable;

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author krwong
 */
@Command(name = "export_objects",
        description = "Export record ids and filenames from a source_files.csv mapping.")
public class ExportObjectsCommand implements Callable<Integer> {
    private static final Logger log = getLogger(ExportObjectsCommand.class);

    @ParentCommand
    private CLIMain parentCommand;

    private MigrationProject project;
    private ExportObjectsService exportObjectsService;

    public void init() throws Exception {
        Path currentPath = parentCommand.getWorkingDirectory();
        project = MigrationProjectFactory.loadMigrationProject(currentPath);
        exportObjectsService = new ExportObjectsService();
        exportObjectsService.setProject(project);
    }

    @Override
    public Integer call() {
        long start = System.nanoTime();
        try {
            init();
            exportObjectsService.exportFilesystemObjects();
            outputLogger.info("Export objects in project {} in {}s", project.getProjectName(),
                    (System.nanoTime() - start) / 1e9);
            return 0;
        } catch (Exception e) {
            log.error("Failed to export objects in {}", project.getProjectName(), e);
            outputLogger.info("Failed to export objects in {}: {}", project.getProjectName(), e.getMessage());
            return 1;
        }
    }
}
