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
package edu.unc.lib.boxc.migration.cdm.services;

import static org.slf4j.LoggerFactory.getLogger;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.regex.Pattern;

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
import edu.unc.lib.boxc.migration.cdm.options.GenerateDestinationMappingOptions;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import edu.unc.lib.boxc.model.api.ids.RepositoryPathConstants;

/**
 * Service for working with destination mappings
 * @author bbpennel
 */
public class DestinationsService {
    private static final Logger log = getLogger(DestinationsService.class);

    public static final String DESTINATION_FIELD = "boxc_dest";
    public static final String COLLECTION_FIELD = "new_collection";
    public static final String ID_FIELD = "id";
    public static final String[] CSV_HEADERS = new String[] {
            ID_FIELD, DESTINATION_FIELD, COLLECTION_FIELD };

    public static final Pattern DEST_PATTERN = Pattern.compile(RepositoryPathConstants.UUID_PATTERN);

    private MigrationProject project;

    /**
     * Generate a destination mapping file for the project using the provided options
     * @param options
     */
    public void generateMapping(GenerateDestinationMappingOptions options) throws IOException {
        assertProjectStateValid();
        assertValidDestination(options.getDefaultDestination());
        ensureMappingState(options.isForce());

        Path fieldsPath = project.getDestinationMappingsPath();

        try (
            BufferedWriter writer = Files.newBufferedWriter(fieldsPath);
            CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(CSV_HEADERS));
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

    private void assertValidDestination(String destination) {
        if (destination == null) {
            return;
        }
        if (!DEST_PATTERN.matcher(destination).matches()) {
            throw new IllegalArgumentException("Invalid destination '" + destination + "', must be a valid UUID");
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
     * @return the destiantion mapping info for the provided project
     * @throws IOException
     */
    public static DestinationsInfo loadMappings(MigrationProject project) throws IOException {
        Path path = project.getDestinationMappingsPath();
        try (
            Reader reader = Files.newBufferedReader(path);
            CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                    .withFirstRecordAsHeader()
                    .withHeader(CSV_HEADERS)
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

    public void setProject(MigrationProject project) {
        this.project = project;
    }
}
