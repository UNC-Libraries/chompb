package edu.unc.lib.boxc.migration.cdm.validators;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo;

/**
 * Validator for source file mappings
 *
 * @author bbpennel
 */
public class SourceFilesValidator {
    protected MigrationProject project;

    public List<String> validateMappings(boolean force) {
        Set<String> previousIds = new HashSet<>();
        Set<String> previousPaths = new HashSet<>();
        List<String> errors = new ArrayList<>();

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
                String id = csvRecord.get(0);
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

                if (StringUtils.isBlank(pathVal)) {
                    if (!force) {
                        errors.add("No path mapped at line " + i);
                    }
                } else {
                    if (previousPaths.contains(pathVal)) {
                        errors.add("Duplicate mapping for path " + pathVal + " at line " + i);
                    } else {
                        try {
                            Path path = Paths.get(pathVal);
                            if (!path.isAbsolute()) {
                                errors.add("Invalid path at line " + i + ", path is not absolute");
                            } else if (Files.exists(path)) {
                                if (Files.isDirectory(path)) {
                                    errors.add("Invalid path at line " + i + ", path is a directory");
                                }
                            } else {
                                errors.add("Invalid path at line " + i + ", file does not exist");
                            }
                        } catch (InvalidPathException e) {
                            errors.add("Invalid path at line " + i + ", not a valid file path");
                        }
                        previousPaths.add(pathVal);
                    }
                }
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

    protected Path getMappingPath() {
        return project.getSourceFilesMappingPath();
    }

    public void setProject(MigrationProject project) {
        this.project = project;
    }
}
