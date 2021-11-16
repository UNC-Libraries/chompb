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

import java.net.URI;
import java.nio.file.Path;
import java.util.concurrent.Callable;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.CdmExportService;
import edu.unc.lib.boxc.migration.cdm.services.CdmFieldService;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

/**
 * @author bbpennel
 */
@Command(name = "export",
        description = "Export records for a collection from CDM")
public class CdmExportCommand implements Callable<Integer> {
    @ParentCommand
    private CLIMain parentCommand;

    @Option(names = { "--cdm-url" },
            description = {"Base URL to the CDM web service API. Falls back to CDM_BASE_URL env variable.",
                    "Default: ${DEFAULT-VALUE}"},
            defaultValue = "${env:CDM_BASE_URL:-http://localhost:82/}")
    private String cdmBaseUri;
    @Option(names = { "-u", "--cdm-user"},
            description = {"User name for CDM requests.",
                    "Defaults to current user: ${DEFAULT-VALUE}"},
            defaultValue = "${sys:user.name}")
    private String cdmUsername;
    @Option(names = {"-p", "--cdm-password"},
            description = "Password for CDM requests. Required.",
            arity = "0..1",
            interactive = true)
    private String cdmPassword;
    @Option(names = {"-n", "-per-page"},
            description = {"Page size for exports.",
                    "Default: ${DEFAULT-VALUE}. Max page size is 5000"},
            defaultValue = "1000")
    private int pageSize;

    private CdmFieldService fieldService;
    private CdmExportService exportService;

    @Override
    public Integer call() throws Exception {
        long start = System.nanoTime();

        try {
            validate();
            initializeServices();

            Path currentPath = parentCommand.getWorkingDirectory();
            MigrationProject project = MigrationProjectFactory.loadMigrationProject(currentPath);
            exportService.exportAll(project);

            outputLogger.info("Exported project {} in {}s", project.getProjectName(),
                    (System.nanoTime() - start) / 1e9);
            return 0;
        } catch (MigrationException e) {
            outputLogger.info("Failed to export project: {}", e.getMessage());
            return 1;
        }
    }

    public void initializeServices() {
        fieldService = new CdmFieldService();
        exportService = new CdmExportService();
        exportService.setCdmBaseUri(cdmBaseUri);
        exportService.setPageSize(pageSize);
        exportService.setCdmFieldService(fieldService);
        initializeAuthenticatedCdmClient();

    }

    private void initializeAuthenticatedCdmClient() {
        if (StringUtils.isBlank(cdmUsername)) {
            throw new MigrationException("Must provided a CDM username");
        }
        if (StringUtils.isBlank(cdmPassword)) {
            throw new MigrationException("Must provided a CDM password for user " + cdmUsername);
        }

        CredentialsProvider credsProvider = new BasicCredentialsProvider();
        outputLogger.info("Initializing connection to {}", URI.create(cdmBaseUri).getHost());
        AuthScope scope = new AuthScope(new HttpHost(URI.create(cdmBaseUri).getHost()));
        credsProvider.setCredentials(scope, new UsernamePasswordCredentials(cdmUsername, cdmPassword));

        CloseableHttpClient httpClient = HttpClients.custom()
                .setDefaultCredentialsProvider(credsProvider)
                .useSystemProperties()
                .build();

        exportService.setHttpClient(httpClient);
    }

    private void validate() {
        if (pageSize < 1 || pageSize > 5000) {
            throw new MigrationException("Page size must be between 1 and 5000");
        }
    }
}
