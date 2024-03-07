package edu.unc.lib.boxc.migration.cdm.validators;

import edu.unc.lib.boxc.auth.api.UserRole;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.PermissionsInfo;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Validator for permission file mappings.
 * @author krwong
 */
public class PermissionsValidator {
    private MigrationProject project;
    private List<String> patronRoles;

    /**
     * Validate the permissions mappings for this project.
     * @return A list of errors. An empty list will be returned for a valid mapping
     */
    public List<String> validateMappings() {
        List<String> errors = new ArrayList<>();
        Path path = project.getPermissionsPath();
        Boolean hasDefault = false;

        try (
            Reader reader = Files.newBufferedReader(path);
            CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                    .withFirstRecordAsHeader()
                    .withHeader(PermissionsInfo.CSV_HEADERS)
                    .withTrim());
        ) {
            int i = 2;
            for (CSVRecord csvRecord : csvParser) {
                if (csvRecord.size() != 4) {
                    errors.add("Invalid entry at line " + i + ", must be 4 columns but were " + csvRecord.size());
                    i++;
                    continue;
                }
                String id = csvRecord.get(0);
                String everyone = csvRecord.get(2);
                String authenticated = csvRecord.get(3);

                // default values
                if (PermissionsInfo.DEFAULT_ID.equals(id)) {
                    if (hasDefault) {
                        errors.add("Can only map default permissions once, encountered reassignment at line " + i);
                    } else {
                        hasDefault = true;
                    }
                }

                // id
                if (StringUtils.isBlank(id)) {
                    errors.add("Invalid blank id at line " + i);
                }

                // everyone
                if (!StringUtils.isBlank(everyone)) {
                    List<String> patronRoles = getPatronRoles();
                    if (!patronRoles.contains(everyone)) {
                        errors.add("Invalid 'everyone' permission at line " + i + ", " + everyone +
                                " is not a valid patron permission");
                    }
                } else {
                    errors.add("No 'everyone' permission mapped at line " + i);
                }

                // authenticated
                if (!StringUtils.isBlank(authenticated)) {
                    List<String> patronRoles = getPatronRoles();
                    if(!patronRoles.contains(authenticated)) {
                        errors.add("Invalid 'authenticated' permission at line " + i + ", " + authenticated +
                                " is not a valid patron permission");
                    }
                } else {
                    errors.add("No 'authenticated' permission mapped at line " + i);
                }

                i++;
            }
            if (i == 2) {
                errors.add("Permission mappings file contained no mappings");
            }
            return errors;
        } catch (IOException | IllegalArgumentException e) {
            throw new MigrationException("Failed to read mappings file", e);
        }
    }

    private List<String> getPatronRoles() {
        if (patronRoles == null) {
            patronRoles = new ArrayList<>();
            List<UserRole> userRoleList = UserRole.getPatronRoles();
            for (UserRole userRole : userRoleList) {
                patronRoles.add(userRole.name());
            }
        }
        return patronRoles;
    }

    public void setProject(MigrationProject project) {
        this.project = project;
    }
}
