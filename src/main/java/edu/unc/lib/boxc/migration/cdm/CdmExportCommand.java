package edu.unc.lib.boxc.migration.cdm;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.CdmExportOptions;
import edu.unc.lib.boxc.migration.cdm.services.CdmExportService;
import edu.unc.lib.boxc.migration.cdm.services.CdmFieldService;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.services.export.ExportProgressService;
import edu.unc.lib.boxc.migration.cdm.services.export.ExportStateService;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.Callable;

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author bbpennel
 */
@Command(name = "export",
        description = { "Export records for a collection from CDM.",
                "If an export operation was started but did not complete, running this command again will "
                    + "resume from where it left off. To force a restart instead, use the --force option."})
public class CdmExportCommand implements Callable<Integer> {
    private static final Logger log = getLogger(CdmExportCommand.class);
    @ParentCommand
    private CLIMain parentCommand;

    @CommandLine.Mixin
    private CdmExportOptions options;

    private CdmFieldService fieldService;
    private CdmExportService exportService;
    private ExportStateService exportStateService;
    private ExportProgressService exportProgressService;
    private MigrationProject project;

    @Override
    public Integer call() throws Exception {
        long start = System.nanoTime();

        try {
            validate();

            Path currentPath = parentCommand.getWorkingDirectory();
            project = MigrationProjectFactory.loadMigrationProject(currentPath);
            initializeServices();

            startOrResumeExport();
            try {
                exportProgressService.startProgressDisplay();
                exportService.exportAll(options);
            } finally {
                exportProgressService.endProgressDisplay();
            }
            outputLogger.info("Exported project {} in {}s", project.getProjectName(),
                    (System.nanoTime() - start) / 1e9);
            return 0;
        } catch (MigrationException | IOException e) {
            log.error("Failed to export project", e);
            outputLogger.info("Failed to export project: {}", e.getMessage());
            return 1;
        }
    }

    public void initializeServices() throws IOException {
        fieldService = new CdmFieldService();
        exportStateService = new ExportStateService();
        exportStateService.setProject(project);
        exportService = new CdmExportService();
        exportService.setProject(project);
        exportService.setCdmFieldService(fieldService);
        exportService.setExportStateService(exportStateService);
        exportService.setChompbConfig(parentCommand.getChompbConfig());
        exportProgressService = new ExportProgressService();
        exportProgressService.setExportStateService(exportStateService);
    }

    private void startOrResumeExport() throws IOException {
        exportStateService.startOrResumeExport(options.isForce());
        if (exportStateService.isResuming()) {
            outputLogger.info("Resuming incomplete export started {} from where it left off...",
                    exportStateService.getState().getStartTime());
        }
    }

    private void validate() {
        if (StringUtils.isBlank(options.getCdmUsername())) {
            throw new MigrationException("Must provided a CDM username");
        }
        if (StringUtils.isBlank(options.getCdmPassword())) {
            throw new MigrationException("Must provided a CDM password for user " + options.getCdmUsername());
        }
    }
}
