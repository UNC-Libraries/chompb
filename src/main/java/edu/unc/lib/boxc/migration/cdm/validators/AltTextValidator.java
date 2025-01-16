package edu.unc.lib.boxc.migration.cdm.validators;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.AltTextInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.AltTextService;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Validator for alt-text mappings
 * @author krwong
 */
public class AltTextValidator {
    private static final Logger log = getLogger(AltTextValidator.class);
    private MigrationProject project;
    protected Set<String> previousIds = new HashSet<>();
    protected List<String> errors = new ArrayList<>();

    public List<String> validateMappings(boolean force) {
        try (
                Reader reader = Files.newBufferedReader(getMappingPath());
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                        .withFirstRecordAsHeader()
                        .withHeader(AltTextInfo.CSV_HEADERS)
                        .withTrim());
        ) {
            int i = 2;
            for (CSVRecord csvRecord : csvParser) {
                if (csvRecord.size() != 2) {
                    errors.add("Invalid entry at line " + i + ", must be 2 columns but were " + csvRecord.size());
                    continue;
                }
                var mapping = AltTextService.recordToMapping(csvRecord);
                String id = mapping.getCdmId();
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

                if (mapping.getAltTextBody() == null || mapping.getAltTextBody().isEmpty()) {
                    if (!force && !allowUnmapped()) {
                        errors.add("No alt-text mapped at line " + i);
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
        return project.getAltTextMappingPath();
    }

    protected boolean allowUnmapped() {
        return false;
    }

    public void setProject(MigrationProject project) {
        this.project = project;
    }
}
