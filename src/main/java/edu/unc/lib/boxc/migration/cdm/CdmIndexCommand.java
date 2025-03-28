package edu.unc.lib.boxc.migration.cdm;

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.options.CdmIndexOptions;
import org.slf4j.Logger;

import edu.unc.lib.boxc.migration.cdm.exceptions.StateAlreadyExistsException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.CdmFieldService;
import edu.unc.lib.boxc.migration.cdm.services.CdmIndexService;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

/**
 * @author bbpennel
 */
@Command(name = "index",
        description = "Populate the index of object records for this project. Must be run after " +
                "exporting source metadata or providing a CSV file.")
public class CdmIndexCommand implements Callable<Integer> {
    private static final Logger log = getLogger(CdmIndexCommand.class);
    @ParentCommand
    private CLIMain parentCommand;

    private CdmFieldService fieldService;
    private CdmIndexService indexService;
    private MigrationProject project;

    @Mixin
    private CdmIndexOptions options;

    @Override
    public Integer call() throws Exception {
        long start = System.nanoTime();

        try {
            initialize();

            // if user provides csv, check that it exists
            if (options.getCsvFile() != null) {
                if (Files.exists(options.getCsvFile())) {
                    CdmFieldInfo csvExportFields = fieldService.retrieveFieldsFromCsv(options.getCsvFile());
                    fieldService.persistFieldsToProject(project, csvExportFields);
                } else {
                    throw new MigrationException("No csv file exists in " + options.getCsvFile());
                }
            }

            indexService.createDatabase(options);
            indexService.index(options);
            // Display any warning messages to user
            if (!indexService.getIndexingWarnings().isEmpty()) {
                indexService.getIndexingWarnings().forEach(msg -> outputLogger.info(msg));
            }
            outputLogger.info("Indexed project {} in {}s", project.getProjectName(),
                    (System.nanoTime() - start) / 1e9);
            return 0;
        } catch (StateAlreadyExistsException e) {
            outputLogger.info("Cannot index project: {}", e.getMessage());
            return 1;
        } catch (Exception e) {
            log.error("Failed to export project", e);
            outputLogger.info("Failed to export project: {}", e.getMessage(), e);
            indexService.removeIndex();
            return 1;
        }
    }

    private void initialize() throws IOException {
        Path currentPath = parentCommand.getWorkingDirectory();
        project = MigrationProjectFactory.loadMigrationProject(currentPath);
        fieldService = new CdmFieldService();
        indexService = new CdmIndexService();
        indexService.setFieldService(fieldService);
        indexService.setProject(project);
    }
}
