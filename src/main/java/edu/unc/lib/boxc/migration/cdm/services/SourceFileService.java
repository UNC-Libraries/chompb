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
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.exceptions.StateAlreadyExistsException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo;
import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo.SourceFileMapping;
import edu.unc.lib.boxc.migration.cdm.options.SourceFileMappingOptions;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;

/**
 * Service for interacting with source files
 * @author bbpennel
 */
public class SourceFileService {
    private static final Logger log = getLogger(SourceFileService.class);
    private static final int FETCH_SIZE = 1000;
    public static final String POTENTIAL_MATCHES_FIELD = "potential_matches";
    public static final String SOURCE_FILE_FIELD = "source_file";
    public static final String EXPORT_MATCHING_FIELD = "matching_value";
    public static final String ID_FIELD = "id";
    public static final String[] CSV_HEADERS = new String[] {
            ID_FIELD, EXPORT_MATCHING_FIELD, SOURCE_FILE_FIELD, POTENTIAL_MATCHES_FIELD };

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

        Pattern fieldMatchingPattern = Pattern.compile(options.getFieldMatchingPattern());

        // Iterate through exported objects in this collection to match against
        Connection conn = null;
        try (
            // Write to system.out if doing a dry run, otherwise write to mappings file
            BufferedWriter writer = options.getDryRun() ?
                    new BufferedWriter(new OutputStreamWriter(System.out)) :
                    Files.newBufferedWriter(getMappingPath());
            CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(CSV_HEADERS));
        ) {
            Path basePath = options.getBasePath();
            // Query for all values of the export field to be used for matching
            conn = indexService.openDbConnection();
            Statement stmt = conn.createStatement();
            stmt.setFetchSize(FETCH_SIZE);
            ResultSet rs = stmt.executeQuery("select cdmid, " + options.getExportField()
                + " from " + CdmIndexService.TB_NAME);
            while (rs.next()) {
                String cdmId = rs.getString(1);
                String dbFilename = rs.getString(2);

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
        if (!options.getDryRun()) {
            setUpdatedDate(Instant.now());
        }
    }

    private Map<String, List<String>> gatherCandidatePaths(SourceFileMappingOptions options) throws IOException {
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
        if (options.getDryRun()) {
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

    private void assertProjectStateValid() {
        if (project.getProjectProperties().getIndexedDate() == null) {
            throw new InvalidProjectStateException("Project must be indexed prior to generating source mappings");
        }
    }

    /**
     * @return the source file mapping info for the provided project
     * @throws IOException
     */
    public SourceFilesInfo loadMappings() throws IOException {
        Path path = getMappingPath();
        try (
            Reader reader = Files.newBufferedReader(path);
            CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                    .withFirstRecordAsHeader()
                    .withHeader(CSV_HEADERS)
                    .withTrim());
        ) {
            SourceFilesInfo info = new SourceFilesInfo();
            List<SourceFileMapping> mappings = info.getMappings();
            for (CSVRecord csvRecord : csvParser) {
                SourceFileMapping mapping = new SourceFileMapping();
                mapping.setCdmId(csvRecord.get(0));
                mapping.setMatchingValue(csvRecord.get(1));
                mapping.setSourcePath(csvRecord.get(2));
                mapping.setPotentialMatches(csvRecord.get(3));
                mappings.add(mapping);
            }
            return info;
        }
    }

    public void setProject(MigrationProject project) {
        this.project = project;
    }

    public void setIndexService(CdmIndexService indexService) {
        this.indexService = indexService;
    }
}
