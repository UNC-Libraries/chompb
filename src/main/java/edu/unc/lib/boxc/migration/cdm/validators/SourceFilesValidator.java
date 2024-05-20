package edu.unc.lib.boxc.migration.cdm.validators;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo.SourceFileMapping;
import edu.unc.lib.boxc.migration.cdm.services.SourceFileService;
import edu.unc.lib.boxc.migration.cdm.services.StreamingMetadataService;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo;

/**
 * Validator for source file mappings.
 * Individual instances are not safe for concurrent usage.
 *
 * @author bbpennel
 */
public class SourceFilesValidator {
    protected MigrationProject project;
    protected StreamingMetadataService streamingMetadataService;
    protected Set<String> previousIds = new HashSet<>();
    protected Set<String> previousPaths = new HashSet<>();
    protected List<String> errors = new ArrayList<>();

    public List<String> validateMappings(boolean force) {
        try (
                Reader reader = Files.newBufferedReader(getMappingPath());
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                        .withFirstRecordAsHeader()
                        .withHeader(SourceFilesInfo.CSV_HEADERS)
                        .withTrim());
            ) {
            int i = 2;
            for (CSVRecord csvRecord : csvParser) {
                if (csvRecord.size() != 4) {
                    errors.add("Invalid entry at line " + i + ", must be 4 columns but were " + csvRecord.size());
                    continue;
                }
                var mapping = SourceFileService.recordToMapping(csvRecord);
                String id = mapping.getCdmId();
                String pathVal = csvRecord.get(2);
                if (StringUtils.isBlank(id)) {
                    if (!force) {
                        errors.add("Invalid blank id at line " + i);
                    }
                } else {
                    if (previousIds.contains(id)) {
                        errors.add("Duplicate mapping for id " + id + " at line " + i);
                    }
                    previousIds.add(id);
                }

                validateSourcePath(i, id, mapping, force);
                i++;
            }
            if (i == 2) {
                errors.add("Mappings file contained no mappings");
            }
        } catch (IOException e) {
            throw new MigrationException("Failed to read mappings file", e);
        }
        return errors;
    }

    protected void validateSourcePath(int i, String id, SourceFileMapping mapping, boolean force) {
        if (!id.isEmpty() && streamingMetadataService.verifyRecordHasStreamingMetadata(id)) {
            String[] streamingMetadata = streamingMetadataService.getStreamingMetadata(id);
            String streamingUrl = "https://durastream.lib.unc.edu/player?spaceId=" + streamingMetadata[1]
                    + "&filename=" + streamingMetadata[0];
            previousPaths.add(streamingUrl);
        } else {
            if (mapping.getSourcePaths() == null || mapping.getSourcePaths().isEmpty()) {
                if (!force && !allowUnmapped()) {
                    errors.add("No path mapped at line " + i);
                }
                return;
            }
            for (var sourcePath: mapping.getSourcePaths()) {
                if (previousPaths.contains(sourcePath.toString())) {
                    errors.add("Duplicate mapping for path " + sourcePath + " at line " + i);
                } else {
                    try {
                        if (!sourcePath.isAbsolute()) {
                            errors.add("Invalid path at line " + i + ", path is not absolute");
                        } else if (Files.exists(sourcePath)) {
                            if (Files.isDirectory(sourcePath)) {
                                errors.add("Invalid path at line " + i + ", path is a directory");
                            }
                        } else {
                            errors.add("Invalid path at line " + i + ", file does not exist");
                        }
                    } catch (InvalidPathException e) {
                        errors.add("Invalid path at line " + i + ", not a valid file path");
                    }
                    previousPaths.add(sourcePath.toString());
                }
            }
        }
    }

    protected Path getMappingPath() {
        return project.getSourceFilesMappingPath();
    }

    public void setProject(MigrationProject project) {
        this.project = project;
    }

    public void setStreamingMetadataService(StreamingMetadataService streamingMetadataService) {
        this.streamingMetadataService = streamingMetadataService;
    }

    protected boolean allowUnmapped() {
        return false;
    }
}
