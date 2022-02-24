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

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.Callable;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.CdmFieldService;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.services.FindingAidService;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

/**
 * Command to initialize new migration projects
 * @author bbpennel
 */
@Command(name = "init",
        description = "Initialize a new CDM migration project")
public class InitializeProjectCommand implements Callable<Integer> {
    @ParentCommand
    private CLIMain parentCommand;

    @Option(names = { "--cdm-url" },
            description = "Base URL to the CDM web service API. Falls back to CDM_BASE_URL env variable. "
                    + "Default: ${DEFAULT-VALUE}",
            defaultValue = "${env:CDM_BASE_URL:-http://localhost:82/}")
    private String cdmBaseUri;
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

        Path currentPath = parentCommand.getWorkingDirectory();
        String projDisplayName = projectName == null ? currentPath.getFileName().toString() : projectName;
        String collId = cdmCollectionId == null ? projDisplayName : cdmCollectionId;

        // Retrieve field information from CDM
        CdmFieldInfo fieldInfo;
        try {
            fieldService.setCdmBaseUri(cdmBaseUri);
            fieldInfo = fieldService.retrieveFieldsForCollection(collId);
        } catch (IOException | MigrationException e) {
            outputLogger.info("Failed to retrieve field information for collection {} in project {}:\n{}",
                    collId, projDisplayName, e.getMessage());
            return 1;
        }

        String username = System.getProperty("user.name");

        // Instantiate the project
        MigrationProject project = null;
        try {
            project = MigrationProjectFactory.createMigrationProject(
                    currentPath, projectName, cdmCollectionId, username);

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
            outputLogger.info("Failed to record finding aid for collection", e);
            return 1;
        }

        outputLogger.info("Initialized project {} in {}s", projDisplayName, (System.nanoTime() - start) / 1e9);
        return 0;
    }
}
