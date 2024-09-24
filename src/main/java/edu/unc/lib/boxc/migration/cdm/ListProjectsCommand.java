package edu.unc.lib.boxc.migration.cdm;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.services.CdmFieldService;
import edu.unc.lib.boxc.migration.cdm.services.CdmIndexService;
import edu.unc.lib.boxc.migration.cdm.services.ListProjectsService;
import edu.unc.lib.boxc.migration.cdm.services.ProjectPropertiesService;
import edu.unc.lib.boxc.migration.cdm.services.SourceFileService;
import org.slf4j.Logger;
import picocli.CommandLine.Option;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

import java.io.IOException;
import java.util.concurrent.Callable;

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author krwong
 */
@Command(name = "list_projects",
        description = "List all chompb projects in a directory.")
public class ListProjectsCommand implements Callable<Integer> {
    private static final Logger log = getLogger(ListProjectsCommand.class);

    @ParentCommand
    private CLIMain parentCommand;

    @Option(names = { "-ia", "--include-archived" },
            description = "")
    private boolean includeArchived;

    private CdmFieldService fieldService;
    private CdmIndexService indexService;
    private ProjectPropertiesService propertiesService;
    private SourceFileService sourceFileService;
    private ListProjectsService listProjectsService;

    @Override
    public Integer call() {
        try {
            initialize();
            ObjectMapper mapper = new ObjectMapper();
            JsonNode listProjects = listProjectsService.listProjects(parentCommand.getWorkingDirectory(), includeArchived);
            String prettyPrintList = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(listProjects);
            outputLogger.info(prettyPrintList);
            return 0;
        } catch(MigrationException | IllegalArgumentException e) {
            outputLogger.info("List projects failed: {}", e.getMessage());
            return 1;
        } catch(Exception e) {
            log.error("Failed to list projects", e);
            outputLogger.info("List projects failed: {}", e.getMessage(), e);
            return 1;
        }
    }

    private void initialize() throws IOException {
        fieldService = new CdmFieldService();
        indexService = new CdmIndexService();
        indexService.setFieldService(fieldService);
        sourceFileService = new SourceFileService();
        sourceFileService.setIndexService(indexService);
        propertiesService = new ProjectPropertiesService();
        listProjectsService = new ListProjectsService();
        listProjectsService.setFieldService(fieldService);
        listProjectsService.setIndexService(indexService);
        listProjectsService.setSourceFileService(sourceFileService);
        listProjectsService.setPropertiesService(propertiesService);
    }
}
