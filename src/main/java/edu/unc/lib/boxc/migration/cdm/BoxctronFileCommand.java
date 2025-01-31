package edu.unc.lib.boxc.migration.cdm;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.BoxctronFileMappingOptions;
import edu.unc.lib.boxc.migration.cdm.options.GenerateSourceFileMappingOptions;
import edu.unc.lib.boxc.migration.cdm.options.Verbosity;
import edu.unc.lib.boxc.migration.cdm.services.AccessFileService;
import edu.unc.lib.boxc.migration.cdm.services.BoxctronFileService;
import edu.unc.lib.boxc.migration.cdm.services.CdmIndexService;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.status.AccessFilesStatusService;
import edu.unc.lib.boxc.migration.cdm.status.SourceFilesSummaryService;
import edu.unc.lib.boxc.migration.cdm.validators.AccessFilesValidator;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author krwong
 */
@Command(name = "boxctron_files",
        description = "Commands related to boxctron access file mappings")
public class BoxctronFileCommand {
    private static final Logger log = getLogger(BoxctronFileCommand.class);

    @ParentCommand
    private CLIMain parentCommand;

    private MigrationProject project;
    private SourceFilesSummaryService summaryService;
    private BoxctronFileService boxctronFileService;

    @Command(name = "generate",
            description = {
                    "Generate the optional boxctron access copy mapping file for this project."})
    public int generate(@Mixin BoxctronFileMappingOptions options) throws Exception {
        long start = System.nanoTime();

        try {
            initialize(options.getDryRun());

            summaryService.capturePreviousState();
            boxctronFileService.generateMapping(options);
            summaryService.summary(parentCommand.getVerbosity());
            outputLogger.info("Boxctron access mapping generated for {} in {}s", project.getProjectName(),
                    (System.nanoTime() - start) / 1e9);
            return 0;
        } catch (MigrationException | IllegalArgumentException e) {
            outputLogger.info("Cannot generate boxctron access mapping: {}", e.getMessage());
            return 1;
        } catch (Exception e) {
            log.error("Failed to map boxctron access files", e);
            outputLogger.info("Failed to map boxctron access files: {}", e.getMessage(), e);
            return 1;
        }
    }

    private void initialize(boolean dryRun) throws IOException {
        Path currentPath = parentCommand.getWorkingDirectory();
        project = MigrationProjectFactory.loadMigrationProject(currentPath);
        CdmIndexService indexService = new CdmIndexService();
        indexService.setProject(project);
        boxctronFileService = new BoxctronFileService();
        boxctronFileService.setIndexService(indexService);
        boxctronFileService.setProject(project);
        summaryService = new SourceFilesSummaryService();
        summaryService.setProject(project);
        summaryService.setDryRun(dryRun);
        summaryService.setSourceFileService(boxctronFileService);
    }
}
