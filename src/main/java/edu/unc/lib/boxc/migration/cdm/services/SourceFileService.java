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

import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.exceptions.StateAlreadyExistsException;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo;
import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo.SourceFileMapping;
import edu.unc.lib.boxc.migration.cdm.options.SourceFileMappingOptions;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static edu.unc.lib.boxc.migration.cdm.services.CdmIndexService.ENTRY_TYPE_COMPOUND_CHILD;
import static edu.unc.lib.boxc.migration.cdm.services.CdmIndexService.ENTRY_TYPE_FIELD;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Service for interacting with source files
 * @author bbpennel
 */
public class SourceFileService {
    private static final Logger log = getLogger(SourceFileService.class);
    private static final int FETCH_SIZE = 1000;

    protected MigrationProject project;
    private CdmIndexService indexService;

    public SourceFileService() {
    }

    /**
     * Generate the source file mapping
     * @param options
     * @throws IOException
     */
    public void generateMapping(SourceFileMappingOptions options) throws IOException {
        assertProjectStateValid();
        ensureMappingState(options);

        // Gather listing of all potential source file paths to match against
        Map<String, List<String>> candidatePaths = gatherCandidatePaths(options);

        Pattern fieldMatchingPattern = buildFieldMatchingPattern(options);

        Path mappingPath = getMappingPath();
        boolean needsMerge = false;
        // If performing an update, start by writing to a new temp mapping file
        if (options.getUpdate() && Files.exists(mappingPath)) {
            mappingPath = getTempMappingPath();
            // Cleanup temp path if it already exists
            Files.deleteIfExists(mappingPath);
            needsMerge = true;
        }

        // Iterate through exported objects in this collection to match against
        Connection conn = null;
        // Write to system.out if doing a dry run, otherwise write to mappings file
        try (var csvPrinter = openMappingsPrinter((options.getDryRun() && !needsMerge) ? null : mappingPath)) {
            Path basePath = options.getBasePath();
            // Query for all values of the export field to be used for matching
            conn = indexService.openDbConnection();
            Statement stmt = conn.createStatement();
            stmt.setFetchSize(FETCH_SIZE);
            // Query for all non-compound objects. If the entry type is null, the object is a individual cdm object
            ResultSet rs = stmt.executeQuery("select " + CdmFieldInfo.CDM_ID + ", " + options.getExportField()
                    + " from " + CdmIndexService.TB_NAME
                    + " where " + ENTRY_TYPE_FIELD + " = '" + ENTRY_TYPE_COMPOUND_CHILD + "'"
                        + " or " + ENTRY_TYPE_FIELD + " is null");
            // Generate source file mapping entry for each returned object
            while (rs.next()) {
                String cdmId = rs.getString(1);
                String dbFilename = rs.getString(2);

                if (options.isPopulateBlank()) {
                    csvPrinter.printRecord(cdmId, null, null, null);
                    continue;
                }
                if (StringUtils.isBlank(dbFilename)) {
                    log.debug("No matching field for object {}", cdmId);
                    csvPrinter.printRecord(cdmId, null, null, null);
                    continue;
                }

                Matcher fieldMatcher = fieldMatchingPattern.matcher(dbFilename);
                if (fieldMatcher.matches()) {
                    String transformed = fieldMatcher.replaceFirst(options.getFilenameTemplate());
                    if (options.isLowercaseTemplate()) {
                        transformed = transformed.toLowerCase();
                    }

                    List<String> paths = candidatePaths.get(transformed);
                    if (paths == null) {
                        log.debug("Transformed field '{}' => '{}' for {} did not match and source filenames",
                                dbFilename, transformed, cdmId);
                        csvPrinter.printRecord(cdmId, dbFilename, null, null);
                    } else {
                        if (paths.size() > 1) {
                            log.debug("Encountered multiple potential matches for {} from field {}", cdmId, dbFilename);
                            String joined = paths.stream()
                                    .map(s -> basePath.resolve(Paths.get(s)).toString())
                                    .collect(Collectors.joining(","));
                            csvPrinter.printRecord(cdmId, dbFilename, null, joined);
                        } else if (paths.size() == 1) {
                            log.debug("Found match for {} from field {}", cdmId, dbFilename);
                            csvPrinter.printRecord(cdmId, dbFilename,
                                    basePath.resolve(Paths.get(paths.get(0))).toString(), null);
                        } else {
                            throw new MigrationException("No paths returned for matching field value " + dbFilename);
                        }
                    }
                } else {
                    log.debug("Field {} for object {} with field {} does not match the field value pattern",
                            options.getExportField(), cdmId, dbFilename);
                    csvPrinter.printRecord(cdmId, dbFilename, null, null);
                }
            }
        } catch (SQLException e) {
            throw new MigrationException("Error interacting with export index", e);
        } finally {
            CdmIndexService.closeDbConnection(conn);
        }

        // Performing update operation with existing mapping, need to merge values
        if (needsMerge) {
            mergeUpdates(options, mappingPath);
        }

        if (!options.getDryRun()) {
            setUpdatedDate(Instant.now());
        }
    }

    private Pattern buildFieldMatchingPattern(SourceFileMappingOptions options) {
        if (options.isPopulateBlank()) {
            return null;
        }
        return Pattern.compile(options.getFieldMatchingPattern());
    }

    private Map<String, List<String>> gatherCandidatePaths(SourceFileMappingOptions options) throws IOException {
        if (options.isPopulateBlank()) {
            return Collections.emptyMap();
        }

        Path basePath = options.getBasePath();
        if (!Files.isDirectory(basePath)) {
            throw new IllegalArgumentException("Base path must be a directory");
        }

        final PathMatcher pathMatcher;
        final String pathPattern;
        if (StringUtils.isBlank(options.getPathPattern())) {
            pathPattern = null;
            pathMatcher = null;
        } else {
            if (Paths.get(options.getPathPattern()).isAbsolute()) {
                throw new IllegalArgumentException("Path pattern must be relative");
            }
            pathPattern = options.getPathPattern();
            pathMatcher = FileSystems.getDefault().getPathMatcher("glob:" + pathPattern);
        }

        // Mapping of filenames to relative paths versus the base path for those files
        Map<String, List<String>> candidatePaths = new HashMap<>();
        Files.walkFileTree(basePath, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
                if (pathPattern == null || pathMatcher.matches(path)) {
                    String filename = path.getFileName().toString();
                    if (options.isLowercaseTemplate()) {
                        filename = filename.toLowerCase();
                    }
                    List<String> paths = candidatePaths.computeIfAbsent(filename, f -> new ArrayList<>());
                    paths.add(basePath.relativize(path).toString());
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc)
                    throws IOException {
                return FileVisitResult.CONTINUE;
            }
        });
        return candidatePaths;
    }

    private void ensureMappingState(SourceFileMappingOptions options) {
        if (options.getDryRun() || options.getUpdate()) {
            return;
        }
        if (Files.exists(getMappingPath())) {
            if (options.isForce()) {
                try {
                    removeMappings();
                } catch (IOException e) {
                    throw new MigrationException("Failed to overwrite mapping file", e);
                }
            } else {
                throw new StateAlreadyExistsException("Cannot create mapping, a file already exists."
                        + " Use the force flag to overwrite.");
            }
        }
    }

    public void removeMappings() throws IOException {
        try {
            Files.delete(getMappingPath());
        } catch (NoSuchFileException e) {
            log.debug("File does not exist, skipping deletion");
        }
        // Clear date property in case it was set
        setUpdatedDate(null);
    }

    protected void setUpdatedDate(Instant timestamp) throws IOException {
        project.getProjectProperties().setSourceFilesUpdatedDate(timestamp);
        ProjectPropertiesSerialization.write(project);
    }

    protected Path getMappingPath() {
        return project.getSourceFilesMappingPath();
    }

    protected Path getTempMappingPath() {
        var mappingPath = getMappingPath();
        return mappingPath.getParent().resolve("~" + mappingPath.getFileName().toString() + "_new");
    }

    private void assertProjectStateValid() {
        if (project.getProjectProperties().getIndexedDate() == null) {
            throw new InvalidProjectStateException("Project must be indexed prior to generating source mappings");
        }
    }

    /**
     * Merge existing mappings with updated mappings, writing to temporary files as intermediates
     * @param options
     * @param updatesPath
     */
    private void mergeUpdates(SourceFileMappingOptions options, Path updatesPath) throws IOException {
        Path originalPath = getMappingPath();
        Path mergedPath = originalPath.getParent().resolve("~" + originalPath.getFileName().toString() + "_merged");
        // Cleanup temp merged path if it already exists
        Files.deleteIfExists(mergedPath);

        // Load the new mappings into memory
        SourceFilesInfo updateInfo = loadMappings(updatesPath);

        // Iterate through the existing mappings, merging in the updated mappings when appropriate
        try (
            var originalParser = openMappingsParser(originalPath);
            // Write to system.out if doing a dry run, otherwise write to mappings file
            var mergedPrinter = openMappingsPrinter(options.getDryRun() ? null : mergedPath);
        ) {
            Set<String> origIds = new HashSet<>();
            for (CSVRecord originalRecord : originalParser) {
                var origMapping = recordToMapping(originalRecord);

                SourceFileMapping updateMapping = updateInfo.getMappingByCdmId(origMapping.getCdmId());
                if (updateMapping == null) {
                    // No updates, so write original
                    writeMapping(mergedPrinter, origMapping);
                } else if (updateMapping.getSourcePath() != null) {
                    if (options.isForce() || origMapping.getSourcePath() == null) {
                        // overwrite entry with updated mapping source path if using force or original didn't have match
                        writeMapping(mergedPrinter, updateMapping);
                    } else {
                        // retain original source path
                        writeMapping(mergedPrinter, origMapping);
                    }
                } else if (updateMapping.getPotentialMatches() != null) {
                    if (origMapping.getSourcePath() != null) {
                        // Prefer existing match, write original
                        writeMapping(mergedPrinter, origMapping);
                    } else {
                        // merge potential matches
                        if (origMapping.getPotentialMatches() != null) {
                            Set<String> merged = Stream.concat(
                                        origMapping.getPotentialMatches().stream(),
                                        updateMapping.getPotentialMatches().stream())
                                    .collect(Collectors.toSet());
                            updateMapping.setPotentialMatches(new ArrayList<>(merged));
                        }
                        // Write entry with updated potential matches
                        writeMapping(mergedPrinter, updateMapping);
                    }
                } else {
                    // No change, retain original
                    writeMapping(mergedPrinter, origMapping);
                }
                origIds.add(originalRecord.get(0));
            }

            // Add in any records that were updated but not present in the original document
            Set<String> updateIds = updateInfo.getMappings().stream()
                    .map(SourceFileMapping::getCdmId).collect(Collectors.toSet());
            updateIds.removeAll(origIds);
            for (String id : updateIds) {
                writeMapping(mergedPrinter, updateInfo.getMappingByCdmId(id));
            }
        }

        // swap the merged mappings to be the main mappings, unless we're doing a dry run
        if (!options.getDryRun()) {
            Files.move(mergedPath, originalPath, StandardCopyOption.REPLACE_EXISTING);
        }
        Files.delete(updatesPath);
    }

    public static void writeMapping(CSVPrinter csvPrinter, SourceFileMapping mapping) throws IOException {
        csvPrinter.printRecord(mapping.getCdmId(), mapping.getMatchingValue(),
                mapping.getSourcePath(), mapping.getPotentialMatchesString());
    }

    /**
     * @return the source file mapping info for the configured project
     * @throws IOException
     */
    public SourceFilesInfo loadMappings() throws IOException {
        return loadMappings(getMappingPath());
    }

    /**
     * @return the source file mapping info for the provided project
     * @throws IOException
     */
    public static SourceFilesInfo loadMappings(Path mappingPath) throws IOException {
        try (var csvParser = openMappingsParser(mappingPath)) {
            SourceFilesInfo info = new SourceFilesInfo();
            List<SourceFileMapping> mappings = info.getMappings();
            for (CSVRecord csvRecord : csvParser) {
                mappings.add(recordToMapping(csvRecord));
            }
            return info;
        }
    }

    public static SourceFileMapping recordToMapping(CSVRecord csvRecord) {
        SourceFileMapping mapping = new SourceFileMapping();
        mapping.setCdmId(csvRecord.get(0));
        mapping.setMatchingValue(csvRecord.get(1));
        mapping.setSourcePath(csvRecord.get(2));
        mapping.setPotentialMatches(csvRecord.get(3));
        return mapping;
    }

    /**
     * @param mappingPath Path of the CSV to read from
     * @return CSVParser for reading from the csv file
     * @throws IOException
     */
    public static CSVParser openMappingsParser(Path mappingPath) throws IOException {
        Reader reader = Files.newBufferedReader(mappingPath);
        return new CSVParser(reader, CSVFormat.DEFAULT
                .withFirstRecordAsHeader()
                .withHeader(SourceFilesInfo.CSV_HEADERS)
                .withTrim());
    }

    /**
     * @param mappingPath Path CSV will output to. If null, then will output to System.out
     * @return CSVPrinter for writing to specified destination
     * @throws IOException
     */
    public static CSVPrinter openMappingsPrinter(Path mappingPath) throws IOException {
        BufferedWriter writer = mappingPath == null ?
                new BufferedWriter(new OutputStreamWriter(System.out)) :
                Files.newBufferedWriter(mappingPath);
        return new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(SourceFilesInfo.CSV_HEADERS));
    }

    public void setProject(MigrationProject project) {
        this.project = project;
    }

    public void setIndexService(CdmIndexService indexService) {
        this.indexService = indexService;
    }
}
