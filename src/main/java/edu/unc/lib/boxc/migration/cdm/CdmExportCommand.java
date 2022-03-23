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

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.CdmExportOptions;
import edu.unc.lib.boxc.migration.cdm.services.CdmExportService;
import edu.unc.lib.boxc.migration.cdm.services.CdmFieldService;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.services.export.ExportProgressService;
import edu.unc.lib.boxc.migration.cdm.services.export.ExportState;
import edu.unc.lib.boxc.migration.cdm.services.export.ExportStateService;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.concurrent.Callable;

import static edu.unc.lib.boxc.migration.cdm.services.export.ExportState.ProgressState;
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
    @CommandLine.Option(names = {"-p", "--cdm-password"},
            description = "Password for CDM requests. Required.",
            arity = "0..1",
            interactive = true)
    private String cdmPassword;

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
        } catch (MigrationException e) {
            log.error("Failed to export project", e);
            outputLogger.info("Failed to export project: {}", e.getMessage());
            return 1;
        }
    }

    public void initializeServices() {
        fieldService = new CdmFieldService();
        exportStateService = new ExportStateService();
        exportStateService.setProject(project);
        exportService = new CdmExportService();
        exportService.setProject(project);
        exportService.setCdmFieldService(fieldService);
        exportService.setExportStateService(exportStateService);
        exportProgressService = new ExportProgressService();
        exportProgressService.setExportStateService(exportStateService);
        initializeAuthenticatedCdmClient();
    }

    private void startOrResumeExport() throws IOException {
        exportStateService.startOrResumeExport(options.isForce());
        if (exportStateService.isResuming()) {
            outputLogger.info("Resuming incomplete export started {} from where it left off...",
                    exportStateService.getState().getStartTime());
            ExportState exportState = exportStateService.getState();
            if (ProgressState.LISTING_OBJECTS.equals(exportState.getProgressState())) {
                outputLogger.info("Resuming listing of object IDs");
            } else {
                outputLogger.info("Listing of object IDs complete");
                if (ProgressState.EXPORTING.equals(exportState.getProgressState())) {
                    outputLogger.info("Resuming export of object records");
                }
            }
        }
    }

    private void initializeAuthenticatedCdmClient() {
        if (StringUtils.isBlank(options.getCdmUsername())) {
            throw new MigrationException("Must provided a CDM username");
        }
        if (StringUtils.isBlank(cdmPassword)) {
            throw new MigrationException("Must provided a CDM password for user " + options.getCdmUsername());
        }

        CredentialsProvider credsProvider = new BasicCredentialsProvider();
        outputLogger.info("Initializing connection to {}", URI.create(options.getCdmBaseUri()).getHost());
        AuthScope scope = new AuthScope(new HttpHost(URI.create(options.getCdmBaseUri()).getHost()));
        credsProvider.setCredentials(scope, new UsernamePasswordCredentials(
                options.getCdmUsername(), cdmPassword));

        CloseableHttpClient httpClient = HttpClients.custom()
                .setDefaultCredentialsProvider(credsProvider)
                .useSystemProperties()
                .build();

        exportService.setHttpClient(httpClient);
    }

    private void validate() {
        if (options.getPageSize() < 1 || options.getPageSize() > CdmExportOptions.MAX_EXPORT_RECORDS_PER_PAGE) {
            throw new MigrationException("Page size must be between 1 and "
                    + CdmExportOptions.MAX_EXPORT_RECORDS_PER_PAGE);
        }
        if (options.getListingPageSize() < 1 || options.getListingPageSize() > CdmExportOptions.MAX_LIST_IDS_PER_PAGE) {
            throw new MigrationException("Listing page size must be between 1 and "
                    + CdmExportOptions.MAX_LIST_IDS_PER_PAGE);
        }
    }
}
