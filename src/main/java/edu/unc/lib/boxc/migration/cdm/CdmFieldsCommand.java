package edu.unc.lib.boxc.migration.cdm;

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;
import static org.slf4j.LoggerFactory.getLogger;

import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.CdmFieldService;
import edu.unc.lib.boxc.migration.cdm.services.CdmIndexService;
import edu.unc.lib.boxc.migration.cdm.services.FieldAssessmentTemplateService;
import edu.unc.lib.boxc.migration.cdm.services.FieldUrlAssessmentService;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import org.slf4j.Logger;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Option;

/**
 * @author bbpennel
 */
@Command(name = "fields",
        description = "Interactions with CDM fields for the current project")
public class CdmFieldsCommand {
    private static final Logger log = getLogger(CdmFieldsCommand.class);
    @ParentCommand
    private CLIMain parentCommand;

    private CdmFieldService fieldService = new CdmFieldService();
    private FieldAssessmentTemplateService templateService = new FieldAssessmentTemplateService();
    private FieldUrlAssessmentService fieldUrlAssessmentService = new FieldUrlAssessmentService();
    private CdmIndexService indexService = new CdmIndexService();

    @Command(name = "validate",
            description = "Validate the cdm_fields.json file for this project")
    public int validateFields() throws Exception {
        try {
            MigrationProject project = MigrationProjectFactory
                    .loadMigrationProject(parentCommand.getWorkingDirectory());
            fieldService.validateFieldsFile(project);
            outputLogger.info("PASS: CDM fields file at path {} is valid", project.getFieldsPath());
            return 0;
        } catch (InvalidProjectStateException e) {
            outputLogger.info("FAIL: {}", e.getMessage());
            return 1;
        } catch (MigrationException e) {
            log.error("Failed to validate fields file", e);
            outputLogger.info("FAIL: Failed to validate fields file: {}", e.getMessage());
            return 1;
        }
    }

    @Command(name = "generate_report",
            description = "Generate the field assessment spreadsheet template for this project.")
    public int generate() throws Exception {
        try {
            MigrationProject project = MigrationProjectFactory
                    .loadMigrationProject(parentCommand.getWorkingDirectory());
            templateService.setCdmFieldService(fieldService);
            templateService.generate(project);
            outputLogger.info("Report generated");
            return 0;
        } catch (MigrationException | IllegalArgumentException e) {
            outputLogger.info("{}", e.getMessage());
            log.error("Failed to generate fields assessment report", e);
            return 1;
        } catch (Exception e) {
            log.error("Failed to generate report", e);
            outputLogger.info("Failed to generate report: {}", e.getMessage());
            return 1;
        }
    }

    @Command(name = "generate_field_url_report",
            description = "Generate an informational list of CDM fields with URLs")
    public int generateFieldsAndUrlsReport(@Option(names = { "-fa", "--finding-aids" },
            description = "Generate finding aid url report") boolean findingAid) throws Exception {
        try {
            MigrationProject project = MigrationProjectFactory
                    .loadMigrationProject(parentCommand.getWorkingDirectory());
            fieldUrlAssessmentService.setProject(project);
            fieldUrlAssessmentService.setIndexService(indexService);
            fieldUrlAssessmentService.setCdmFieldService(fieldService);
            indexService.setProject(project);
            fieldUrlAssessmentService.generateReport(findingAid);
            outputLogger.info("Fields with URLs report generated!");
            return 0;
        } catch (MigrationException | IllegalArgumentException e) {
            outputLogger.info("{}", e.getMessage());
            log.error("Failed to generate fields with URLs report", e);
            return 1;
        } catch (Exception e) {
            log.error("Failed to generate fields with URLs report", e);
            outputLogger.info("Failed to generate fields with URLs report: {}", e.getMessage());
            return 1;
        }
    }
}
