package edu.unc.lib.boxc.migration.cdm;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.FindingAidReportOptions;
import edu.unc.lib.boxc.migration.cdm.services.CdmFieldService;
import edu.unc.lib.boxc.migration.cdm.services.CdmIndexService;
import edu.unc.lib.boxc.migration.cdm.services.FindingAidReportService;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import org.slf4j.Logger;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.ParentCommand;

import java.io.IOException;
import java.nio.file.Path;

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author krwong
 */
@Command(name = "finding_aid_reports",
        description = "Commands related to finding_aid_reports")
public class FindingAidReportCommand {
    private static final Logger log = getLogger(FindingAidReportCommand.class);

    @ParentCommand
    private CLIMain parentCommand;

    private MigrationProject project;
    private CdmFieldService fieldService;
    private CdmIndexService indexService;
    private FindingAidReportService reportService;

    @Command(name = "field_report",
            description = {"Generate the report of unique values and counts for a given field."})
    public int fieldReport(@Mixin FindingAidReportOptions options) throws Exception {
        long start = System.nanoTime();

        try {
            initialize();
            reportService.fieldCountUniqueValuesReport(options);
            outputLogger.info("Generated finding aid field {} report for {} in {}s", options.getField(),
                    project.getProjectName(), (System.nanoTime() - start) / 1e9);
            return 0;
        } catch (MigrationException | IllegalArgumentException e) {
            outputLogger.info("Cannot generate finding aid field report: {}", e.getMessage());
            return 1;
        } catch (Exception e) {
            log.error("Failed to generate finding aid field report", e);
            outputLogger.info("Failed to generate finding aid field report: {}", e.getMessage(), e);
            return 1;
        }
    }

    @Command(name = "hookid_report",
            description = {"Generate the report of finding aid hookids and lists counts",
            "Combines descri and contri values, separated with an underscore (descri_contri)."})
    public int hookIdReport() throws Exception {
        long start = System.nanoTime();

        try {
            initialize();
            reportService.hookIdReport();
            outputLogger.info("Generated hookId report for {} in {}s", project.getProjectName(),
                    (System.nanoTime() - start) / 1e9);
            return 0;
        } catch (MigrationException | IllegalArgumentException e) {
            outputLogger.info("Cannot generate hookId report: {}", e.getMessage());
            return 1;
        } catch (Exception e) {
            log.error("Failed to generate hookId report", e);
            outputLogger.info("Failed to generate hookId report: {}", e.getMessage(), e);
            return 1;
        }
    }

    @Command(name = "collection_report",
            description = {"Generate the report of finding aid hookids and lists counts"})
    public int collectionReport() throws Exception {
        long start = System.nanoTime();

        try {
            initialize();
            reportService.collectionReport();
            outputLogger.info("Generated collection report for {} in {}s", project.getProjectName(),
                    (System.nanoTime() - start) / 1e9);
            return 0;
        } catch (MigrationException | IllegalArgumentException e) {
            outputLogger.info("Cannot generate collection report: {}", e.getMessage());
            return 1;
        } catch (Exception e) {
            log.error("Failed to generate collection report", e);
            outputLogger.info("Failed to generate collection report: {}", e.getMessage(), e);
            return 1;
        }
    }

    private void initialize() throws IOException {
        Path currentPath = parentCommand.getWorkingDirectory();
        project = MigrationProjectFactory.loadMigrationProject(currentPath);
        fieldService = new CdmFieldService();
        fieldService.setProject(project);
        indexService = new CdmIndexService();
        indexService.setProject(project);
        reportService = new FindingAidReportService();
        reportService.setProject(project);
        reportService.setFieldService(fieldService);
        reportService.setIndexService(indexService);
    }
}
