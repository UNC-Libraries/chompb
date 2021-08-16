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
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import org.slf4j.Logger;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.MigrationSip;
import edu.unc.lib.boxc.migration.cdm.options.SipGenerationOptions;
import edu.unc.lib.boxc.migration.cdm.services.AccessFileService;
import edu.unc.lib.boxc.migration.cdm.services.CdmIndexService;
import edu.unc.lib.boxc.migration.cdm.services.DescriptionsService;
import edu.unc.lib.boxc.migration.cdm.services.DestinationsService;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.services.SipService;
import edu.unc.lib.boxc.migration.cdm.services.SourceFileService;
import edu.unc.lib.boxc.model.api.ids.PIDMinter;
import edu.unc.lib.boxc.model.fcrepo.ids.RepositoryPIDMinter;
import edu.unc.lib.boxc.operations.impl.events.PremisLoggerFactoryImpl;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.ParentCommand;

/**
 * @author bbpennel
 */
@Command(name = "sips",
        description = "Commands related to submission information packages")
public class SipsCommand {
    private static final Logger log = getLogger(SipsCommand.class);
    @ParentCommand
    private CLIMain parentCommand;

    private MigrationProject project;
    private SourceFileService sourceFileService;
    private AccessFileService accessFileService;
    private DescriptionsService descriptionsService;
    private DestinationsService destinationsService;
    private CdmIndexService indexService;
    private PIDMinter pidMinter;
    private PremisLoggerFactoryImpl premisLoggerFactory;
    private SipService sipService;

    @Command(name = "generate",
            description = "Generate SIPs for this project")
    public int generate(@Mixin SipGenerationOptions options) throws Exception {
        long start = System.nanoTime();

        try {
            initialize();

            List<MigrationSip> sips = sipService.generateSips(options);
            for (MigrationSip sip : sips) {
                outputLogger.info("Generated SIP for deposit with ID {}", sip.getDepositPid().getId());
                outputLogger.info("    * SIP path: {}", sip.getSipPath());
                if (sip.getNewCollectionPid() != null) {
                    outputLogger.info("    * Added new collection {} with box-c id {}",
                            sip.getNewCollectionLabel(), sip.getNewCollectionId());
                }
            }
            outputLogger.info("Completed operation for project {} in {}s", project.getProjectName(),
                    (System.nanoTime() - start) / 1e9);
            return 0;
        } catch (IllegalArgumentException e) {
            outputLogger.info("Cannot generate SIPs: {}", e.getMessage());
            return 1;
        } catch (MigrationException e) {
            log.error("Failed to generate SIPs", e);
            outputLogger.info("Cannot generate SIPs: {}", e.getCause() != null ?
                    e.getCause().getMessage() : e.getMessage());
            return 1;
        } catch (Exception e) {
            log.error("Failed to generate SIPs", e);
            outputLogger.info("Failed to generate SIPs: {}", e.getMessage(), e);
            return 1;
        }
    }

    @Command(name = "list",
            description = "List the SIPs that have been generated for this project")
    public int generate() throws Exception {
        try {
            initialize();

            List<MigrationSip> sips = sipService.listSips();
            outputLogger.info("Listing {} SIP(s)", sips.size());
            outputLogger.info("========================");
            for (MigrationSip sip : sips) {
                outputLogger.info("SIP/Deposit ID: {}", sip.getDepositId());
                outputLogger.info("    Path: {}", sip.getSipPath());
                if (sip.getNewCollectionPid() != null) {
                    outputLogger.info("    New collection: {} ({})",
                            sip.getNewCollectionLabel(), sip.getNewCollectionId());
                }
            }
            return 0;
        } catch (Exception e) {
            log.error("Failed to list SIPs", e);
            outputLogger.info("Failed to list SIPs: {}", e.getMessage(), e);
            return 1;
        }
    }

    private void initialize() throws IOException {
        Path currentPath = parentCommand.getWorkingDirectory();
        project = MigrationProjectFactory.loadMigrationProject(currentPath);

        pidMinter = new RepositoryPIDMinter();
        premisLoggerFactory = new PremisLoggerFactoryImpl();
        premisLoggerFactory.setPidMinter(pidMinter);
        indexService = new CdmIndexService();
        indexService.setProject(project);
        sourceFileService = new SourceFileService();
        sourceFileService.setIndexService(indexService);
        sourceFileService.setProject(project);
        accessFileService = new AccessFileService();
        accessFileService.setIndexService(indexService);
        accessFileService.setProject(project);
        descriptionsService = new DescriptionsService();
        descriptionsService.setProject(project);
        destinationsService = new DestinationsService();
        destinationsService.setProject(project);

        sipService = new SipService();
        sipService.setIndexService(indexService);
        sipService.setAccessFileService(accessFileService);
        sipService.setSourceFileService(sourceFileService);
        sipService.setPidMinter(pidMinter);
        sipService.setDescriptionsService(descriptionsService);
        sipService.setPremisLoggerFactory(premisLoggerFactory);
        sipService.setProject(project);
    }
}
