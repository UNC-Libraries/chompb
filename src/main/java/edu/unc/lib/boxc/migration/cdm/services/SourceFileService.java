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
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.SourceFileMappingOptions;

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

    private MigrationProject project;
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

        // Gather listing of all potential source file paths to match against
        Map<String, List<String>> candidatePaths = gatherCandidatePaths(options);

        Pattern fieldMatchingPattern = Pattern.compile(options.getFieldMatchingPattern());

        // Iterate through exported objects in this collection to match against
        Connection conn = null;
        try (
            // Write to system.out if doing a dry run, otherwise write to mappings file
            BufferedWriter writer = options.getDryRun() ?
                    new BufferedWriter(new OutputStreamWriter(System.out)) :
                    Files.newBufferedWriter(project.getSourceFilesMappingPath());
            CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(CSV_HEADERS));
        ){
            // Query for all values of the export field to be used for matching
            conn = indexService.openDbConnection();
            PreparedStatement stmt = conn.prepareStatement("select cdmid, ? from ?");
            stmt.setString(1, options.getExportField());
            stmt.setString(2, CdmIndexService.TB_NAME);
            stmt.setFetchSize(FETCH_SIZE);
            ResultSet rs = stmt.executeQuery();
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

                    List<String> paths = candidatePaths.get(transformed);
                    if (paths.size() > 1) {
                        log.debug("Encountered multiple potential matches for {} from field {}", cdmId, dbFilename);
                        csvPrinter.printRecord(cdmId, dbFilename, null, String.join(",", paths));
                    } else if (paths.size() == 1) {
                        log.debug("Found match for {} from field {}", cdmId, dbFilename);
                        csvPrinter.printRecord(cdmId, dbFilename, paths.get(0), null);
                    } else {
                        throw new MigrationException("No paths returned for matching field value " + dbFilename);
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
    }

    private Map<String, List<String>> gatherCandidatePaths(SourceFileMappingOptions options) throws IOException {
        Path basePath = options.getBasePath();
        if (!Files.isDirectory(basePath)) {
            throw new IllegalArgumentException("Base path must be a directory");
        }

        final String pathPattern;
        if (StringUtils.isBlank(options.getPathPattern())) {
            pathPattern = null;
        } else {
            if (Paths.get(options.getPathPattern()).isAbsolute()) {
                throw new IllegalArgumentException("Path pattern must be relative");
            }
            pathPattern = options.getPathPattern();
        }

        // Mapping of filenames to relative paths versus the base path for those files
        Map<String, List<String>> candidatePaths = new HashMap<>();
        PathMatcher pathMatcher = FileSystems.getDefault().getPathMatcher(pathPattern);
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

    private void assertProjectStateValid() {
        if (project.getProjectProperties().getIndexedDate() == null) {
            throw new InvalidProjectStateException("Project must be indexed prior to generating source mappings");
        }
    }

    public void setProject(MigrationProject project) {
        this.project = project;
    }

    public void setIndexService(CdmIndexService indexService) {
        this.indexService = indexService;
    }
}
