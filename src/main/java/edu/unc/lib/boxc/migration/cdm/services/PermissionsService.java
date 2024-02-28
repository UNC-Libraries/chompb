package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.auth.api.UserRole;
import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.exceptions.StateAlreadyExistsException;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
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
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Service for permission mapping
 * @author krwong
 */
public class PermissionsService {
    private static final Logger log = getLogger(PermissionsService.class);

    private MigrationProject project;
    private CdmIndexService indexService;
    private List<String> patronRoles;

    /**
     * Generates permission mapping csv
     * @param options permission mapping options
     */
    public void generatePermissions(PermissionMappingOptions options) throws Exception {
        Path permissionsPath = project.getPermissionsPath();
        ensureMappingState(options.isForce());

        String everyoneField;
        String authenticatedField;

        // Permissions
        if (options.isStaffOnly() || options.getEveryone() != null || options.getAuthenticated() != null) {
            everyoneField = getAssignedRoleValue(options.isStaffOnly(), options.getEveryone());
            authenticatedField = getAssignedRoleValue(options.isStaffOnly(), options.getAuthenticated());
        } else {
            // if no permissions/roles are specified, default to canViewOriginals
            everyoneField = UserRole.canViewOriginals.getPredicate();
            authenticatedField = UserRole.canViewOriginals.getPredicate();
        }

        try (
                BufferedWriter writer = Files.newBufferedWriter(permissionsPath);
                CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(PermissionsInfo.CSV_HEADERS));
        ) {
            if (options.isWithDefault()) {
                csvPrinter.printRecord(PermissionsInfo.DEFAULT_ID,
                        everyoneField,
                        authenticatedField);
            }

            List<String> mappedIds = queryForMappedIds(options);
            for (String id : mappedIds) {
                csvPrinter.printRecord(id,
                        everyoneField,
                        authenticatedField);
            }
        }

        ProjectPropertiesSerialization.write(project);
    }

    /**
     * Update existing permission mapping csv
     * @param options permission mapping options
     */
    public void setPermissions(PermissionMappingOptions options) throws Exception {
        if (!doesIdExistInIndex(options.getCdmId())) {
            throw new IllegalArgumentException("Id " + options.getCdmId() + " does not exist in this project.");
        }

        Path permissionsMappingPath = project.getPermissionsPath();
        if (!Files.exists(permissionsMappingPath)) {
            throw new InvalidProjectStateException("Permissions csv does not exist.");
        }

        // add or update permission for a specific cdmId
        List<List<String>> records = updateCsvRecords(options);

        try (
            BufferedWriter writer = Files.newBufferedWriter(permissionsMappingPath);
            CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(PermissionsInfo.CSV_HEADERS));
        ) {
            for (List<String> record : records) {
                csvPrinter.printRecord(record.get(0), record.get(1), record.get(2));
            }
        }
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

    private String getAssignedRoleValue(boolean isStaffOnly, UserRole role) {
        String roleValue;
        List<String> patronRoles = getPatronRoles();

        if (isStaffOnly) {
            roleValue = UserRole.none.getPredicate();
        } else if (role.isPatronRole()) {
            roleValue = role.getPredicate();
        } else {
            throw new IllegalArgumentException("Assigned role value is invalid. " +
                    "Must be one of the following patron roles: " + patronRoles);
        }
        return roleValue;
    }

    private List<String> getIds(String query) {
        List<String> ids = new ArrayList<>();

        getIndexService();
        try (Connection conn = indexService.openDbConnection()) {
            var stmt = conn.createStatement();
            var rs = stmt.executeQuery(query);
            while (rs.next()) {
                if (!rs.getString(1).isEmpty()) {
                    ids.add(rs.getString(1));
                }
            }
            return ids;
        } catch (SQLException e) {
            throw new MigrationException("Error interacting with export index", e);
        }
    }

    private List<String> queryForMappedIds(PermissionMappingOptions options) {
        List<String> mappedIds = new ArrayList<>();

        // works and files
        if (options.isWithWorks() && options.isWithFiles()) {
            String workAndFileQuery = "select distinct " + CdmFieldInfo.CDM_ID +
                    " from " + CdmIndexService.TB_NAME;
            mappedIds = getIds(workAndFileQuery);
        }

        // works
        if (options.isWithWorks() && !options.isWithFiles()) {
            // for every work in the project (grouped works, compound objects, and single file works)
            String workQuery = "select distinct " + CdmFieldInfo.CDM_ID
                    + " from " + CdmIndexService.TB_NAME
                    + " where " + CdmIndexService.ENTRY_TYPE_FIELD + " = '" + CdmIndexService.ENTRY_TYPE_GROUPED_WORK + "'"
                    + " or " + CdmIndexService.ENTRY_TYPE_FIELD + " = '" + CdmIndexService.ENTRY_TYPE_COMPOUND_OBJECT + "'"
                    + " or " + CdmIndexService.ENTRY_TYPE_FIELD + " is null"
                    + " and " + CdmIndexService.PARENT_ID_FIELD + " is null";
            mappedIds = getIds(workQuery);
        }

        // files
        if (options.isWithFiles() && !options.isWithWorks()) {
            // for every file in the project (compound children and grouped children)
            // If the entry type is null, the object is a individual cdm object
            String fileQuery = "select distinct " + CdmFieldInfo.CDM_ID +
                    " from " + CdmIndexService.TB_NAME
                    + " where " + CdmIndexService.ENTRY_TYPE_FIELD + " = '" + CdmIndexService.ENTRY_TYPE_COMPOUND_CHILD + "'"
                    + " or " + CdmIndexService.ENTRY_TYPE_FIELD + " is null"
                    + " and " + CdmIndexService.PARENT_ID_FIELD + " is not null";
            mappedIds = getIds(fileQuery);
        }

        return mappedIds;
    }

    private boolean doesIdExistInIndex(String id) {
        String query = "select " + CdmFieldInfo.CDM_ID + " from " + CdmIndexService.TB_NAME
                + " where " + CdmFieldInfo.CDM_ID + " = ?";

        getIndexService();
        try (Connection conn = indexService.openDbConnection()) {
            var stmt = conn.prepareStatement(query);
            stmt.setString(1, id);
            var rs = stmt.executeQuery();
            while (rs.next()) {
                if (!rs.getString(1).isEmpty()) {
                    return true;
                }
            }
            return false;
        } catch (SQLException e) {
            throw new MigrationException("Error interacting with export index", e);
        }
    }

    private List<List<String>> updateCsvRecords(PermissionMappingOptions options) {
        List<CSVRecord> previousRecords = getPermissions();
        List<List<String>> updatedRecords = new ArrayList<>();
        Set<String> cdmIds = new HashSet<>();
        String everyoneField = getAssignedRoleValue(options.isStaffOnly(), options.getEveryone());
        String authenticatedField = getAssignedRoleValue(options.isStaffOnly(), options.getAuthenticated());

        // update existing entry
        for (CSVRecord record : previousRecords) {
            cdmIds.add(record.get(0));
            if (record.get(0).equals(options.getCdmId())) {
                updatedRecords.add(Arrays.asList(record.get(0), everyoneField, authenticatedField));
            } else {
                updatedRecords.add(Arrays.asList(record.get(0), record.get(1), record.get(2)));
            }
        }

        // add new entry
        if (!cdmIds.contains(options.getCdmId())) {
            updatedRecords.add(Arrays.asList(options.getCdmId(), everyoneField, authenticatedField));
        }

        return updatedRecords;
    }

    public List<CSVRecord> getPermissions() {
        List<CSVRecord> permissions = new ArrayList<>();
        try (
                Reader reader = Files.newBufferedReader(project.getPermissionsPath());
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                        .withFirstRecordAsHeader()
                        .withHeader(PermissionsInfo.CSV_HEADERS)
                        .withTrim());
        ) {
            permissions = csvParser.getRecords();
        } catch (IOException e) {
            log.error("Failed to list permissions", e);
        }

        return permissions;
    }

    private CdmIndexService getIndexService() {
        if (indexService == null) {
            indexService = new CdmIndexService();
            indexService.setProject(project);
        }
        return indexService;
    }

    public void setProject(MigrationProject project) {
        this.project = project;
    }

    public void setIndexService(CdmIndexService indexService) {
        this.indexService = indexService;
    }
}
