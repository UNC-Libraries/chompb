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
package edu.unc.lib.boxc.migration.cdm.validators;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.DestinationsInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.model.api.ids.RepositoryPathConstants;

/**
 * Utility for validating destination mappings
 *
 * @author bbpennel
 */
public class DestinationsValidator {

    public static final Pattern DEST_PATTERN = Pattern.compile(RepositoryPathConstants.UUID_PATTERN);

    private MigrationProject project;

    /**
     * Validate the destination mappings for this project.
     * @param force if true, incomplete, overlapping and duplicate mappings will be ignored
     * @return A list of errors. An empty list will be returned for a valid mapping
     */
    public List<String> validateMappings(boolean force) {
        Map<String, String> collIdToDest = new HashMap<>();
        Set<String> destsWithCollId = new HashSet<>();
        Set<String> destsWithoutCollId = new HashSet<>();
        Set<String> previousIds = new HashSet<>();
        List<String> errors = new ArrayList<>();
        Path path = project.getDestinationMappingsPath();
        String defaultDest = null;
        String defaultColl = null;
        try (
            Reader reader = Files.newBufferedReader(path);
            CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                    .withFirstRecordAsHeader()
                    .withHeader(DestinationsInfo.CSV_HEADERS)
                    .withTrim());
        ) {
            int i = 2;
            for (CSVRecord csvRecord : csvParser) {
                if (csvRecord.size() != 3) {
                    errors.add("Invalid entry at line " + i + ", must be 3 columns but were " + csvRecord.size());
                    continue;
                }
                String id = csvRecord.get(0);
                String dest = csvRecord.get(1);
                String collId = csvRecord.get(2);
                boolean rowIsDefault = false;

                if (DestinationsInfo.DEFAULT_ID.equals(id)) {
                    if (defaultDest != null) {
                        errors.add("Can only map default destination once, encountered reassignment at line " + i);
                    } else {
                        defaultDest = dest;
                        defaultColl = collId;
                        if (!force && (destsWithCollId.contains(dest) || destsWithoutCollId.contains(dest))) {
                            errors.add("Default destination " + dest + " is already mapped directly to objects");
                        }
                    }
                    rowIsDefault = true;
                }

                if (StringUtils.isBlank(id)) {
                    if (!force) {
                        errors.add("Invalid blank id at line " + i);
                    }
                } else if (!rowIsDefault) {
                    if (previousIds.contains(id)) {
                        errors.add("Object ID assigned to multiple destinations, see line " + i);
                    } else {
                        previousIds.add(id);
                    }
                }

                if (!StringUtils.isBlank(dest)) {
                    if (!isValidDestination(dest)) {
                        errors.add("Invalid destination at line " + i + ", " + dest + " is not a valid UUID");
                    } else if (!rowIsDefault && dest.equals(defaultDest) && collId.equals(defaultColl)) {
                        if (!force) {
                            errors.add("Destination at line " + i + " is already mapped as default");
                        }
                    }

                    if (!StringUtils.isBlank(collId)) {
                        if (destsWithoutCollId.contains(dest)) {
                            errors.add("Destination at line " + i
                                    + " has been previously mapped without a new collection");
                        } else {
                            destsWithCollId.add(dest);
                        }
                    } else {
                        if (destsWithCollId.contains(dest)) {
                            errors.add("Destination at line " + i
                                    + " has been previously mapped with a new collection");
                        } else {
                            destsWithoutCollId.add(dest);
                        }
                    }
                } else if (!force) {
                    errors.add("No destination mapped at line " + i);
                }

                if (!StringUtils.isBlank(collId)) {
                    String mappedDest = collIdToDest.get(collId);
                    if (mappedDest != null) {
                        if (!mappedDest.equals(dest)) {
                            errors.add("New collection ID " + collId + " cannot be associated with "
                                    + "multiple destinations, but is mapped to both " + mappedDest + " and " + dest);
                        }
                    } else {
                        collIdToDest.put(collId, dest);
                    }
                }

                i++;
            }
            if (i == 2) {
                errors.add("Destination mappings file contained no mappings");
            }
            return errors;
        } catch (IOException | IllegalArgumentException e) {
            throw new MigrationException("Failed to read mappings file", e);
        }
    }

    /**
     * Assert that the provided destination is valid
     * @param destination
     */
    public static void assertValidDestination(String destination) {
        if (destination == null) {
            return;
        }
        if (!isValidDestination(destination)) {
            throw new IllegalArgumentException("Invalid destination '" + destination + "', must be a valid UUID");
        }
    }

    /**
     * @param destination
     * @return true if the provided destination is valid
     */
    public static boolean isValidDestination(String destination) {
        return DEST_PATTERN.matcher(destination).matches();
    }

    public void setProject(MigrationProject project) {
        this.project = project;
    }
}
