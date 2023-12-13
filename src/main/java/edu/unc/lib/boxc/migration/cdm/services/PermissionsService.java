package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.auth.api.UserRole;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.exceptions.StateAlreadyExistsException;
import edu.unc.lib.boxc.migration.cdm.model.DestinationsInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.PermissionsInfo;
import edu.unc.lib.boxc.migration.cdm.options.PermissionMappingOptions;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Service for permission mapping
 * @author krwong
 */
public class PermissionsService {
    private static final Logger log = getLogger(PermissionsService.class);

    private MigrationProject project;
    private String everyoneField;
    private String authenticatedField;
    private List<String> patronRoles;

    /**
     * Generates default mapping csv
     * @param options default mapping options
     */
    public void generateDefaultPermissions(PermissionMappingOptions options) throws Exception {
        Path fieldsPath = project.getPermissionsPath();
        ensureMappingState(options.isForce());

        List<String> patronRoles = getPatronRoles();

        // Everyone permission
        if (options.isStaffOnly()) {
            everyoneField = "none";
        } else if (options.getEveryone() != null) {
            if (patronRoles.contains(options.getEveryone())) {
                everyoneField = options.getEveryone();
            } else {
                throw new IllegalArgumentException("Everyone value is invalid. " +
                        "Must be one of the following patron roles: " + patronRoles);
            }
        } else {
            // if no permissions/roles are specified, default to canViewOriginals
            everyoneField = "canViewOriginals";
        }

        // Authenticated permission
        if (options.isStaffOnly()) {
            authenticatedField = "none";
        } else if (options.getAuthenticated() != null) {
            if (patronRoles.contains(options.getAuthenticated())) {
                authenticatedField = options.getAuthenticated();
            } else {
                throw new IllegalArgumentException("Authenticated value is invalid. " +
                        "Must be one of the following patron roles: " + patronRoles);
            }
        } else {
            // if no permissions/roles are specified, default to canViewOriginals
            authenticatedField = "canViewOriginals";
        }

        try (
                BufferedWriter writer = Files.newBufferedWriter(fieldsPath);
                CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(PermissionsInfo.CSV_HEADERS));
        ) {
            if (options.getCdmId() != null && options.getCdmId().matches("default")) {
                csvPrinter.printRecord(DestinationsInfo.DEFAULT_ID,
                        everyoneField,
                        authenticatedField);
            }
        }

        ProjectPropertiesSerialization.write(project);
    }

    /**
     * @param project
     * @return the permissions mapping info for the provided project
     * @throws IOException
     */
    public static PermissionsInfo loadMappings(MigrationProject project) throws IOException {
        Path path = project.getPermissionsPath();
        try (
                Reader reader = Files.newBufferedReader(path);
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                        .withFirstRecordAsHeader()
                        .withHeader(PermissionsInfo.CSV_HEADERS)
                        .withTrim());
        ) {
            PermissionsInfo info = new PermissionsInfo();
            List<PermissionsInfo.PermissionMapping> mappings = info.getMappings();
            for (CSVRecord csvRecord : csvParser) {
                PermissionsInfo.PermissionMapping mapping = new PermissionsInfo.PermissionMapping();
                mapping.setId(csvRecord.get(0));
                mapping.setEveryone(csvRecord.get(1));
                mapping.setAuthenticated(csvRecord.get(2));
                mappings.add(mapping);
            }
            return info;
        }
    }

    public void removeMappings() throws IOException {
        try {
            Files.delete(project.getPermissionsPath());
        } catch (NoSuchFileException e) {
            log.debug("File does not exist, skipping deletion");
        }
        ProjectPropertiesSerialization.write(project);
    }

    private void ensureMappingState(boolean force) {
        if (Files.exists(project.getPermissionsPath())) {
            if (force) {
                try {
                    removeMappings();
                } catch (IOException e) {
                    throw new MigrationException("Failed to overwrite permissions file", e);
                }
            } else {
                throw new StateAlreadyExistsException("Cannot create permissions, a file already exists."
                        + " Use the force flag to overwrite.");
            }
        }
    }

    private List<String> getPatronRoles() {
        if (patronRoles == null) {
            patronRoles = new ArrayList<>();
            List<UserRole> userRoleList = UserRole.getPatronRoles();
            for (UserRole userRole : userRoleList) {
                patronRoles.add(userRole.getPredicate());
            }
        }
        return patronRoles;
    }

    public void setProject(MigrationProject project) {
        this.project = project;
    }
}
