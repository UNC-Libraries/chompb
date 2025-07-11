package edu.unc.lib.boxc.migration.cdm.validators;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.AspaceRefIdInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Validator for aspace ref id mappings
 * @author krwong
 */
public class AspaceRefIdValidator {
    private static final Logger log = getLogger(AspaceRefIdValidator.class);
    private MigrationProject project;
    protected Set<String> previousIds = new HashSet<>();
    protected List<String> errors = new ArrayList<>();

    public static final String REF_ID_QUERY = "[a-zA-Z0-9]{32}";
    public static final Pattern PATTERN = Pattern.compile(REF_ID_QUERY);

    public List<String> validateMappings(boolean force) {
        try (
                Reader reader = Files.newBufferedReader(getMappingPath());
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                        .withFirstRecordAsHeader()
                        .withHeader(AspaceRefIdInfo.CSV_HEADERS)
                        .withTrim());
        ) {
            int i = 2;
            for (CSVRecord csvRecord : csvParser) {
                if (!(csvRecord.size() == 2 || csvRecord.size() == 3)) {
                    errors.add("Invalid entry at line " + i + ", must be 2 or 3 columns but were " + csvRecord.size());
                    continue;
                }
                String id = csvRecord.get(0);
                String refId = csvRecord.get(2);

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

                if (!StringUtils.isBlank(refId)) {
                    Matcher matcher = PATTERN.matcher(refId);
                    if (!matcher.matches()) {
                        errors.add("Invalid ref id at line " + i);
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
        return project.getAspaceRefIdMappingPath();
    }

    public void setProject(MigrationProject project) {
        this.project = project;
    }
}
