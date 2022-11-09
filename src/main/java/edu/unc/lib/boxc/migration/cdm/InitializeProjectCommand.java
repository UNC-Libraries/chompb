/**
 * Copyright 2008 The University of North Carolina at Chapel Hill
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
        var cdmEnvConfig = config.getCdmEnvironments().get(cdmEnvId);

        Path currentPath = parentCommand.getWorkingDirectory();
        String projDisplayName = projectName == null ? currentPath.getFileName().toString() : projectName;
        String collId = cdmCollectionId == null ? projDisplayName : cdmCollectionId;

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

        String username = System.getProperty("user.name");

        // Instantiate the project
        MigrationProject project = null;
        try {
            project = MigrationProjectFactory.createMigrationProject(
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
}
