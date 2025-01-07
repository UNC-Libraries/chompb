package edu.unc.lib.boxc.migration.cdm;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.AltTextOptions;
import edu.unc.lib.boxc.migration.cdm.services.AltTextService;
import edu.unc.lib.boxc.migration.cdm.services.CdmIndexService;
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
@Command(name = "alt_text",
        description = "Commands related to alt-text")
public class AltTextCommand {
    private static final Logger log = getLogger(AltTextCommand.class);

    @ParentCommand
    private CLIMain parentCommand;

    private MigrationProject project;
    private CdmIndexService indexService;
    private AltTextService altTextService;

    @Command(name = "upload",
            description = {"Upload a CSV or txt files to the alt_text folder.",
                    "Txt files will be copied to the alt_text folder.",
                    "Alt-text txt files will be created from each row of the CSV, excluding the header row. " +
                    "CSV file must contain a header row and all following rows should have comma-separated" +
                    "dmrecord and alt-text values."})
    public int upload(@Mixin AltTextOptions options) throws Exception {
        initialize();
        try {
            if (options.getAltTextCsvFile() != null) {
                altTextService.uploadCsv(options);
            } else if (options.getAltTextTxtFiles() != null) {
                altTextService.uploadTxtFiles(options);
            }
            return 0;
        } catch (MigrationException | IllegalArgumentException e) {
            outputLogger.info("Cannot upload alt-text file(s): {}", e.getMessage());
            return 1;
        } catch (Exception e) {
            log.error("Failed to upload alt-text file(s)", e);
            outputLogger.info("Failed to upload alt-text file(s): {}", e.getMessage(), e);
            return 1;
        }
    }

    @Command(name="template",
            description = {"Create an alt-text csv template with headers 'dmrecord' and 'alt-text' and " +
                    "the 'dmrecord' column populated with dmrecord values from the collection. " +
                    "'alt-text' values must be manually inputted."})
    public int template(@Mixin AltTextOptions options) throws Exception {
        initialize();
        try {
            altTextService.generateTemplate();
            return 0;
        } catch (MigrationException | IllegalArgumentException e) {
            outputLogger.info("Cannot generate alt-text csv template: {}", e.getMessage());
            return 1;
        } catch (Exception e) {
            log.error("Failed to generate alt-text csv template", e);
            outputLogger.info("Failed to generate alt-text csv template: {}", e.getMessage(), e);
            return 1;
        }
    }

    private void initialize() throws IOException {
        Path currentPath = parentCommand.getWorkingDirectory();
        project = MigrationProjectFactory.loadMigrationProject(currentPath);
        indexService = new CdmIndexService();
        indexService.setProject(project);
        altTextService = new AltTextService();
        altTextService.setProject(project);
        altTextService.setIndexService(indexService);
    }
}
