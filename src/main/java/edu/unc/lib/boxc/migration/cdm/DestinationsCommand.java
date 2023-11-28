package edu.unc.lib.boxc.migration.cdm;

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import edu.unc.lib.boxc.migration.cdm.services.ArchivalDestinationsService;
import edu.unc.lib.boxc.migration.cdm.services.CdmFieldService;
import edu.unc.lib.boxc.migration.cdm.services.CdmIndexService;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.DestinationMappingOptions;
import edu.unc.lib.boxc.migration.cdm.options.Verbosity;
import edu.unc.lib.boxc.migration.cdm.services.DestinationsService;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.status.DestinationsStatusService;
import edu.unc.lib.boxc.migration.cdm.validators.DestinationsValidator;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

/**
 * @author bbpennel
 */
@Command(name = "destinations",
        description = "Commands related to destination mappings")
public class DestinationsCommand {
    private static final Logger log = getLogger(DestinationsCommand.class);
    @ParentCommand
    private CLIMain parentCommand;

    private MigrationProject project;
    private DestinationsService destService;
    private ArchivalDestinationsService archivalDestService;
    private CdmIndexService indexService;
    private CdmFieldService fieldService;

    @Command(name = "generate",
            description = "Generate the destination mapping file for this project")
    public int generate(@Mixin DestinationMappingOptions options) throws Exception {
        long start = System.nanoTime();

        try {
            validateOptions(options);
            initialize();

            destService.generateMapping(options);
            outputLogger.info("Destination mapping generated for {} in {}s", project.getProjectName(),
                    (System.nanoTime() - start) / 1e9);
            return 0;
        } catch (MigrationException | IllegalArgumentException e) {
            outputLogger.info("Cannot generate mappings: {}", e.getMessage());
            return 1;
        } catch (Exception e) {
            log.error("Failed to export project", e);
            outputLogger.info("Failed to export project: {}", e.getMessage(), e);
            return 1;
        }
    }

    @Command(name = "validate",
            description = "Validate the destination mapping file for this project")
    public int validate(@Option(names = { "-f", "--force"},
            description = "Ignore incomplete, overlapping and duplicate mappings") boolean force) throws Exception {
        try {
            initialize();
            DestinationsValidator validator = new DestinationsValidator();
            validator.setProject(project);
            List<String> errors = validator.validateMappings(force);
            if (errors.isEmpty()) {
                outputLogger.info("PASS: Destination mapping at path {} is valid",
                        project.getDestinationMappingsPath());
                return 0;
            } else {
                if (parentCommand.getVerbosity().equals(Verbosity.QUIET)) {
                    outputLogger.info("FAIL: Destination mapping is invalid with {} errors", errors.size());
                } else {
                    outputLogger.info("FAIL: Destination mapping at path {} is invalid due to the following issues:",
                            project.getDestinationMappingsPath());
                    for (String error : errors) {
                        outputLogger.info("    - " + error);
                    }
                }
                return 1;
            }
        } catch (MigrationException e) {
            log.error("Failed to validate destination mappings", e);
            outputLogger.info("FAIL: Failed to validate destination mappings: {}", e.getMessage());
            return 1;
        }
    }

    @Command(name = "status",
            description = "Display status of the destination mappings for this project")
    public int status() throws Exception {
        try {
            initialize();
            DestinationsStatusService statusService = new DestinationsStatusService();
            statusService.setProject(project);
            statusService.report(parentCommand.getVerbosity());

            return 0;
        } catch (MigrationException | IllegalArgumentException e) {
            outputLogger.info("Status failed: {}", e.getMessage());
            return 1;
        } catch (Exception e) {
            log.error("Status failed", e);
            outputLogger.info("Status failed: {}", e.getMessage(), e);
            return 1;
        }
    }

    @Command(name = "add",
            description = "Add custom destination for individual CDM ID or list of IDs")
    public int add(@Mixin DestinationMappingOptions options) throws Exception {
        initialize();
        var destinationMappingExists = Files.exists(project.getDestinationMappingsPath());
        if (!destinationMappingExists) {
          outputLogger.info("FAIL: Destination mapping at path {} does not exist",
                  project.getDestinationMappingsPath());
          return 1;
        }
        try {
            validateOptions(options);
            destService.addMappings(options);
            return 0;
        } catch (MigrationException | IllegalArgumentException e) {
            outputLogger.info("Cannot add mappings: {}", e.getMessage());
            return 1;
        }
    }

    @Command(name="map_archival_collections",
            description = "Generate the destination mappings file by matching CDM field values to " +
                    "the archival collection number in boxc")
    public int generateArchival(@Mixin DestinationMappingOptions options) throws Exception {
        long start = System.nanoTime();

        try {
            validateArchivalOptions(options);
            initialize();
            fieldService = new CdmFieldService();
            indexService = new CdmIndexService();
            indexService.setProject(project);
            indexService.setFieldService(fieldService);

            archivalDestService = new ArchivalDestinationsService();
            archivalDestService.setProject(project);
            archivalDestService.setIndexService(indexService);
            archivalDestService.setDestinationsService(destService);
            archivalDestService.setSolrServerUrl(parentCommand.getChompbConfig().getBxcEnvironments()
                    .get(project.getProjectProperties().getBxcEnvironmentId()).getSolrServerUrl());
            archivalDestService.initialize();

            archivalDestService.addArchivalCollectionMappings(options);
            outputLogger.info("Archival destination mapping generated for {} in {}s", project.getProjectName(),
                    (System.nanoTime() - start) / 1e9);
            return 0;
        } catch (MigrationException | IllegalArgumentException e) {
            outputLogger.info("Cannot generate archival mappings: {}", e);
            return 1;
        } catch (Exception e) {
            log.error("Failed to export project", e);
            outputLogger.info("Failed to export project: {}", e.getMessage(), e);
            return 1;
        }
    }

    private void validateOptions(DestinationMappingOptions options) {
        // For now, the only kind of mapping is a default, so fail if not set
        if (StringUtils.isBlank(options.getDefaultDestination())) {
            throw new IllegalArgumentException("Must provide a default destination");
        }
        if (options.getDefaultCollection() != null && StringUtils.isBlank(options.getDefaultCollection())) {
            throw new IllegalArgumentException("Default collection must not be blank");
        }
        if (options.getCdmId() != null && StringUtils.isBlank(options.getCdmId())) {
            throw new IllegalArgumentException("CDM ID must not be blank");
        }
    }

    private void validateArchivalOptions(DestinationMappingOptions options) {
        if (StringUtils.isBlank(options.getFieldName())) {
            throw new IllegalArgumentException("Must provide a field name");
        }
        if (options.getDefaultDestination() != null && StringUtils.isBlank(options.getDefaultDestination())) {
            throw new IllegalArgumentException("Default destination must not be blank");
        }
        if (options.getDefaultCollection() != null && StringUtils.isBlank(options.getDefaultCollection())) {
            throw new IllegalArgumentException("Default collection must not be blank");
        }
    }

    private void initialize() throws IOException {
        Path currentPath = parentCommand.getWorkingDirectory();
        project = MigrationProjectFactory.loadMigrationProject(currentPath);
        destService = new DestinationsService();
        destService.setProject(project);
    }
}
