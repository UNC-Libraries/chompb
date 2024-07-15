package edu.unc.lib.boxc.migration.cdm;

import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.CdmFieldService;
import edu.unc.lib.boxc.migration.cdm.services.ChompbConfigService.ChompbConfig;
import edu.unc.lib.boxc.migration.cdm.services.FindingAidService;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.Callable;

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Command to initialize new migration projects
 * @author bbpennel
 */
@Command(name = "init",
        description = "Initialize a new CDM migration project")
public class InitializeProjectCommand implements Callable<Integer> {
    private static final Logger log = getLogger(InitializeProjectCommand.class);
    @ParentCommand
    private CLIMain parentCommand;

    @Option(names = { "-e", "--cdm-env" },
            description = "CDM environment used for retrieving data. Env-config must be set. "
                    + "Default: ${DEFAULT-VALUE}",
            defaultValue = "${env:CDM_ENV:-test}")
    private String cdmEnvId;
    @Option(names = { "-E", "--bxc-env" },
            description = "Box-c environment used as the migration destination. Env-config must be set. "
                    + "Default: ${DEFAULT-VALUE}",
            defaultValue = "${env:BXC_ENV:-test}")
    private String bxcEnvId;
    @Option(names = { "-c", "--cdm-coll-id" },
            description = "Identifier of the CDM collection to migrate. Use if the name of the project directory"
                    + " does not match the CDM Collection ID.")
    private String cdmCollectionId;
    @Option(names = { "-p", "--project-name" },
            description = {
                "If specified, a new directory named with the provided value will be created for the project.",
                "If not specified, then the project will be initialized in the current directory.",
                "If the project name is different from the CDM collection ID, then use -c to specify the ID." })
    private String projectName;
    @Option(names = {"-s", "--source"},
            description = {"Specify the source of migration data when initializing a new project. Accepted values: " +
                    "cdm (CDM collection), files (filesystem, doesn't need to be linked with CDM collection)." +
                    "Defaults to cdm."},
            defaultValue = "cdm")
    private String projectSource;

    private CdmFieldService fieldService;
    private CloseableHttpClient httpClient;
    private FindingAidService findingAidService;

    public InitializeProjectCommand() {
        httpClient = HttpClients.createMinimal();
        fieldService = new CdmFieldService();
        fieldService.setHttpClient(httpClient);
        findingAidService = new FindingAidService();
    }

    @Override
    public Integer call() throws Exception {
        long start = System.nanoTime();

        ChompbConfig config;
        try {
            config = parentCommand.getChompbConfig();
            if (!config.getCdmEnvironments().containsKey(cdmEnvId)) {
                outputLogger.info("Unknown cdm-env value {}, configured values are: {}",
                        cdmEnvId, String.join(", ", config.getCdmEnvironments().keySet()));
                return 1;
            }
            if (!config.getBxcEnvironments().containsKey(bxcEnvId)) {
                outputLogger.info("Unknown bxc-env value {}, configured values are: {}",
                        bxcEnvId, String.join(", ", config.getBxcEnvironments().keySet()));
                return 1;
            }
        } catch (IllegalArgumentException | IOException e) {
            outputLogger.info("Unable to read application configuration: {}", e.getMessage());
            log.error("Unable to read application configuration", e);
            return 1;
        }

        Path currentPath = parentCommand.getWorkingDirectory();
        String projDisplayName = projectName == null ? currentPath.getFileName().toString() : projectName;
        Integer integer = null;

        if (projectSource.equalsIgnoreCase("cdm")) {
            integer = initCdmProject(config, currentPath, projDisplayName, start);
        } else if (projectSource.equalsIgnoreCase("files")) {
            integer = initFilesProject(currentPath, projDisplayName, start);
        }

        return integer;
    }

    private Integer initCdmProject(ChompbConfig config, Path currentPath, String projDisplayName, long start) throws Exception {
        var cdmEnvConfig = config.getCdmEnvironments().get(cdmEnvId);
        String collId = cdmCollectionId == null ? projDisplayName : cdmCollectionId;
        String username = System.getProperty("user.name");

        // Retrieve field information from CDM
        CdmFieldInfo fieldInfo;
        try {
            fieldService.setCdmBaseUri(cdmEnvConfig.getHttpBaseUrl());
            fieldInfo = fieldService.retrieveFieldsForCollection(collId);
        } catch (IOException | MigrationException e) {
            log.error("Failed to retrieve field information for collection in project", e);
            outputLogger.info("Failed to retrieve field information for collection {} in project {}:\n{}",
                    collId, projDisplayName, e.getMessage());
            return 1;
        }

        // Instantiate the project
        MigrationProject project = null;
        try {
            project = MigrationProjectFactory.createCdmMigrationProject(
                    currentPath, projectName, cdmCollectionId, username, cdmEnvId, bxcEnvId);

            // Persist field info to the project
            fieldService.persistFieldsToProject(project, fieldInfo);
        } catch (InvalidProjectStateException e) {
            outputLogger.info("Failed to initialize project {}: {}", projDisplayName, e.getMessage());
            return 1;
        }

        //Record collection's finding aid (if available)
        try {
            findingAidService.setProject(project);
            findingAidService.setCdmFieldService(fieldService);
            findingAidService.recordFindingAid();
        } catch (Exception e) {
            log.error("Failed to record finding aid for collection", e);
            outputLogger.info("Failed to record finding aid for collection", e);
            return 1;
        }

        outputLogger.info("Initialized project {} in {}s", projDisplayName, (System.nanoTime() - start) / 1e9);
        return 0;
    }

    private Integer initFilesProject(Path currentPath, String projDisplayName, long start) throws Exception {
        String username = System.getProperty("user.name");

        try {
            MigrationProjectFactory.createFilesMigrationProject(
                    currentPath, projectName, null, username,  bxcEnvId, projectSource);
        } catch (InvalidProjectStateException e) {
            outputLogger.info("Failed to initialize project {}: {}", projDisplayName, e.getMessage());
            return 1;
        }

        outputLogger.info("Initialized project {} in {}s", projDisplayName, (System.nanoTime() - start) / 1e9);
        return 0;
    }
}
