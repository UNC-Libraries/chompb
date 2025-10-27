package edu.unc.lib.boxc.migration.cdm;

import static edu.unc.lib.boxc.migration.cdm.services.CdmFieldService.CSV;
import static edu.unc.lib.boxc.migration.cdm.services.CdmFieldService.EAD_TO_CDM;
import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.options.CdmIndexOptions;
import edu.unc.lib.boxc.migration.cdm.services.FileIndexService;
import edu.unc.lib.boxc.migration.cdm.services.IndexService;
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
    private CdmIndexService cdmIndexService;
    private FileIndexService fileIndexService;
    private IndexService indexService;
    private MigrationProject project;

    @Mixin
    private CdmIndexOptions options;

    @Override
    public Integer call() throws Exception {
        long start = System.nanoTime();

        try {
            initialize();

            if (options.getCsvFile() != null && options.getEadTsvFile() != null) {
                throw new IllegalArgumentException("CSVs and EAD to CDM TSVs may not be used in the same indexing command");
            }

            // if user provides a file, check that it exists
            if (options.getCsvFile() != null) {
                if (Files.exists(options.getCsvFile())) {
                    CdmFieldInfo csvExportFields = fieldService.retrieveFields(options.getCsvFile(), CSV);
                    fieldService.persistFieldsToProject(project, csvExportFields);
                    indexService.createDatabase(options);
                    fileIndexService.setSource(CSV);
                    fileIndexService.indexAllFromFile(options);
                } else {
                    throw new MigrationException("No csv file exists in " + options.getCsvFile());
                }
            } else if (options.getEadTsvFile() != null) { // if user provides EAD to CDM tsv, check that it exists
                if (Files.exists(options.getEadTsvFile())) {
                    // standardize headers and add CDM ID column
                    var formattedEadToCdmTsvPath = fileIndexService.addIdsToEadToCdmTsv(options.getEadTsvFile());
                    CdmFieldInfo csvExportFields = fieldService.retrieveFields(formattedEadToCdmTsvPath, EAD_TO_CDM);
                    fieldService.persistFieldsToProject(project, csvExportFields);
                    indexService.createDatabase( options);
                    fileIndexService.setSource(EAD_TO_CDM);
                    fileIndexService.indexAllFromFile(options);
                } else {
                    throw new MigrationException("No EAD to CDM tsv file exists in " + options.getEadTsvFile());
                }
            } else {
                indexService.createDatabase(options);
                cdmIndexService.index(options);
                // Display any warning messages to user
                if (!cdmIndexService.getIndexingWarnings().isEmpty()) {
                    cdmIndexService.getIndexingWarnings().forEach(outputLogger::info);
                }
            }
            outputLogger.info("Indexed project {} in {}s", project.getProjectName(),
                    (System.nanoTime() - start) / 1e9);
            return 0;
        } catch (StateAlreadyExistsException e) {
            outputLogger.info("Cannot index project: {}", e.getMessage());
            return 1;
        } catch (IllegalArgumentException e) {
            outputLogger.info("Command arguments are invalid: {}", e.getMessage());
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
        cdmIndexService = new CdmIndexService();
        cdmIndexService.setFieldService(fieldService);
        cdmIndexService.setProject(project);
        fileIndexService = new FileIndexService();
        fileIndexService.setFieldService(fieldService);
        fileIndexService.setProject(project);
        indexService = new IndexService();
        indexService.setProject(project);
        indexService.setFieldService(fieldService);
    }
}
