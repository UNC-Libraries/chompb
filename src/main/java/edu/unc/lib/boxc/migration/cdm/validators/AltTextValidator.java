package edu.unc.lib.boxc.migration.cdm.validators;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.AltTextService;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Validator for alt-text CSV file to upload
 * @author krwong
 */
public class AltTextValidator {
    protected MigrationProject project;
    protected Set<String> previousIds = new HashSet<>();
    protected List<String> errors = new ArrayList<>();

    public List<String> validateCsv(Path csvPath, boolean force) {
        try (
                Reader reader = Files.newBufferedReader(csvPath);
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                        .withFirstRecordAsHeader()
                        .withHeader(AltTextService.CSV_HEADERS)
                        .withTrim());
        ) {
            int i = 2;
            for (CSVRecord csvRecord : csvParser) {
                if (csvRecord.size() != 2) {
                    errors.add("Invalid entry at line " + i + ", must be 2 columns but were " + csvRecord.size());
                    continue;
                }
                String id = csvRecord.get(0);
                String altText = csvRecord.get(1);
                if (StringUtils.isBlank(id)) {
                    if (!force) {
                        errors.add("Invalid blank id at line " + i);
                    }
                } else {
                    if (previousIds.contains(id)) {
                        errors.add("Duplicate id " + id + " at line " + i);
                    }
                    previousIds.add(id);
                }

                validateAltText(i, altText, force);
                i++;
            }
            if (i == 2) {
                errors.add("Alt-text CSV contained no alt-text");
            }
        } catch (IOException e) {
            throw new MigrationException("Failed to read alt-text file", e);
        }
        return errors;
    }

    private void validateAltText(int i, String altText, boolean force) {
        if (altText == null || altText.isEmpty()) {
            if (!force) {
                errors.add("No alt-text at line " + i);
            }
        }
    }

    public void setProject(MigrationProject project) {
        this.project = project;
    }
}
