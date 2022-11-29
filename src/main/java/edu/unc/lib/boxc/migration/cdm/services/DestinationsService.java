package edu.unc.lib.boxc.migration.cdm.services;

import static org.slf4j.LoggerFactory.getLogger;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;

import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.exceptions.StateAlreadyExistsException;
import edu.unc.lib.boxc.migration.cdm.model.DestinationsInfo;
import edu.unc.lib.boxc.migration.cdm.model.DestinationsInfo.DestinationMapping;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.DestinationMappingOptions;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import edu.unc.lib.boxc.migration.cdm.validators.DestinationsValidator;

/**
 * Service for working with destination mappings
 * @author bbpennel
 */
public class DestinationsService {
    private static final Logger log = getLogger(DestinationsService.class);

    private MigrationProject project;

    /**
     * Generate a destination mapping file for the project using the provided options
     * @param options
     */
    public void generateMapping(DestinationMappingOptions options) throws IOException {
        assertProjectStateValid();
        DestinationsValidator.assertValidDestination(options.getDefaultDestination());
        ensureMappingState(options.isForce());

        Path fieldsPath = project.getDestinationMappingsPath();

        try (
            BufferedWriter writer = Files.newBufferedWriter(fieldsPath);
            CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(DestinationsInfo.CSV_HEADERS));
        ) {
            if (options.getDefaultDestination() != null) {
                csvPrinter.printRecord(DestinationsInfo.DEFAULT_ID,
                        options.getDefaultDestination(),
                        options.getDefaultCollection());
            }
        }

        project.getProjectProperties().setDestinationsGeneratedDate(Instant.now());
        ProjectPropertiesSerialization.write(project);
    }

    private void assertProjectStateValid() {
        if (project.getProjectProperties().getIndexedDate() == null) {
            throw new InvalidProjectStateException("Project must be indexed prior to generating destinations");
        }
    }

    private void ensureMappingState(boolean force) {
        if (Files.exists(project.getDestinationMappingsPath())) {
            if (force) {
                try {
                    removeMappings();
                } catch (IOException e) {
                    throw new MigrationException("Failed to overwrite destinations file", e);
                }
            } else {
                throw new StateAlreadyExistsException("Cannot create destinations, a file already exists."
                        + " Use the force flag to overwrite.");
            }
        }
    }

    public void removeMappings() throws IOException {
        try {
            Files.delete(project.getDestinationMappingsPath());
        } catch (NoSuchFileException e) {
            log.debug("File does not exist, skipping deletion");
        }
        // Clear date property in case it was set
        project.getProjectProperties().setDestinationsGeneratedDate(null);
        ProjectPropertiesSerialization.write(project);
    }

    /**
     * @param project
     * @return the destination mapping info for the provided project
     * @throws IOException
     */
    public static DestinationsInfo loadMappings(MigrationProject project) throws IOException {
        Path path = project.getDestinationMappingsPath();
        try (
            Reader reader = Files.newBufferedReader(path);
            CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                    .withFirstRecordAsHeader()
                    .withHeader(DestinationsInfo.CSV_HEADERS)
                    .withTrim());
        ) {
            DestinationsInfo info = new DestinationsInfo();
            List<DestinationMapping> mappings = info.getMappings();
            for (CSVRecord csvRecord : csvParser) {
                DestinationMapping mapping = new DestinationMapping();
                mapping.setId(csvRecord.get(0));
                mapping.setDestination(csvRecord.get(1));
                mapping.setCollectionId(csvRecord.get(2));
                mappings.add(mapping);
            }
            return info;
        }
    }

    /**
     * Adds custom CDM ID mapping(s) to destination mappings file
     * @param options
     * @throws Exception
     */
    public void addMappings(DestinationMappingOptions options) throws Exception {
        assertProjectStateValid();
        DestinationsValidator.assertValidDestination(options.getDefaultDestination());

        Path destinationMappingsPath = project.getDestinationMappingsPath();

        try (
                BufferedWriter writer = Files.newBufferedWriter(destinationMappingsPath,
                                                                StandardOpenOption.APPEND,
                                                                StandardOpenOption.CREATE);
                CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.Builder.create().build());
        ) {
            if (options.getDefaultDestination() != null) {
                // value passed in for cdm ID might be a comma-delimited list, so split it first and loop through
                var cdmIds = options.getCdmId().split(",");
                for (String cdmId : cdmIds) {
                    csvPrinter.printRecord(cdmId,
                            options.getDefaultDestination(),
                            options.getDefaultCollection());
                }
            }
        }
    }

    public void setProject(MigrationProject project) {
        this.project = project;
    }
}
