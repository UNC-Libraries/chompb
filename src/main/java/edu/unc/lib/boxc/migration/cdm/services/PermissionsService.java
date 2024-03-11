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
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Service for permission mapping
 * @author krwong
 */
public class PermissionsService {
    private static final Logger log = getLogger(PermissionsService.class);
    public static final String WORK_OBJECT_TYPE = "work";
    public static final String FILE_OBJECT_TYPE = "file";
    private static final String WORK_QUERY = "select " + CdmFieldInfo.CDM_ID
            + " from " + CdmIndexService.TB_NAME
            + " where " + CdmIndexService.ENTRY_TYPE_FIELD + " = '" + CdmIndexService.ENTRY_TYPE_GROUPED_WORK + "'"
            + " or " + CdmIndexService.ENTRY_TYPE_FIELD + " = '" + CdmIndexService.ENTRY_TYPE_COMPOUND_OBJECT + "'"
            + " or " + CdmIndexService.ENTRY_TYPE_FIELD + " is null"
            + " and " + CdmIndexService.PARENT_ID_FIELD + " is null";

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
                        "",
                        everyoneField,
                        authenticatedField);
            }

            List<Map.Entry<String, String>> mappedIdsAndObjectType = queryForMappedIds(options);
            for (Map.Entry<String, String> entry : mappedIdsAndObjectType) {
                csvPrinter.printRecord(entry.getKey(),
                        entry.getValue(),
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
        if (options.getCdmId() != null && !doesIdExistInIndex(options.getCdmId())) {
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
                csvPrinter.printRecord(record.get(0), record.get(1), record.get(2), record.get(3));
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
                mapping.setEveryone(csvRecord.get(2));
                mapping.setAuthenticated(csvRecord.get(3));
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

    private List<Map.Entry<String, String>> queryForMappedIds(PermissionMappingOptions options) {
        List<String> mappedIds;
        List<Map.Entry<String, String>> mappedIdsAndObjType = new ArrayList<>();

        // works
        if (options.isWithWorks()) {
            // for every work in the project (grouped works, compound objects, and single file works)
            mappedIds = getIds(WORK_QUERY);
            for (String id : mappedIds) {
                mappedIdsAndObjType.add(new AbstractMap.SimpleEntry<>(id, WORK_OBJECT_TYPE));
            }
        }

        // files
        if (options.isWithFiles()) {
            // for every file in the project (compound children and grouped children)
            // If the entry type is null, the object is a individual cdm object
            String fileQuery = "select " + CdmFieldInfo.CDM_ID +
                    " from " + CdmIndexService.TB_NAME
                    + " where " + CdmIndexService.ENTRY_TYPE_FIELD + " = '" + CdmIndexService.ENTRY_TYPE_COMPOUND_CHILD + "'"
                    + " or " + CdmIndexService.ENTRY_TYPE_FIELD + " is null"
                    + " and " + CdmIndexService.PARENT_ID_FIELD + " is not null";
            mappedIds = getIds(fileQuery);
            for (String id : mappedIds) {
                mappedIdsAndObjType.add(new AbstractMap.SimpleEntry<>(id, FILE_OBJECT_TYPE));
            }
        }

        return mappedIdsAndObjType.stream().sorted(Map.Entry.comparingByKey()).collect(Collectors.toList());
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

    private String getObjectType(String id) {
        String query = WORK_QUERY + " and " + CdmFieldInfo.CDM_ID + " = '" + id + "'";

        getIndexService();
        try (Connection conn = indexService.openDbConnection()) {
            var stmt = conn.createStatement();
            var rs = stmt.executeQuery(query);
            while (rs.next()) {
                if (!rs.getString(1).isEmpty()) {
                    return WORK_OBJECT_TYPE;
                }
            }
            return FILE_OBJECT_TYPE;
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

        // add or update individual entry
        if (options.getCdmId() != null) {
            // update existing entry
            for (CSVRecord record : previousRecords) {
                cdmIds.add(record.get(0));
                if (record.get(0).equals(options.getCdmId())) {
                    updatedRecords.add(Arrays.asList(record.get(0), record.get(1), everyoneField, authenticatedField));
                } else {
                    updatedRecords.add(Arrays.asList(record.get(0), record.get(1), record.get(2), record.get(3)));
                }
            }
            // add new entry
            if (!cdmIds.contains(options.getCdmId())) {
                String objectType = getObjectType(options.getCdmId());
                updatedRecords.add(Arrays.asList(options.getCdmId(), objectType, everyoneField, authenticatedField));
            }
        }

        // add or update with-works and with-files entries
        if (options.isWithWorks() || options.isWithFiles()) {
            List<Map.Entry<String, String>> addWorkFileRecords = queryForMappedIds(options);
            Set<String> previousIds = previousRecords.stream().map(entry ->
                    entry.get(PermissionsInfo.ID_FIELD)).collect(Collectors.toSet());
            Set<String> workFileIds = addWorkFileRecords.stream().map(Map.Entry::getKey).collect(Collectors.toSet());
            // removed updated entries from list of previous entries and add unchanged previous entries to updatedRecords
            previousIds.removeAll(workFileIds);
            for (CSVRecord record : previousRecords) {
                if (previousIds.contains(record.get(0))) {
                    updatedRecords.add(Arrays.asList(record.get(0), record.get(1), record.get(2), record.get(3)));
                }
            }
            // add works or files to updatedRecords (includes updated entries and new entries)
            for (Map.Entry<String, String> workFileRecord : addWorkFileRecords) {
                updatedRecords.add(Arrays.asList(workFileRecord.getKey(), workFileRecord.getValue(), everyoneField, authenticatedField));
            }
            Collections.sort(updatedRecords, Comparator.comparing(e -> e.get(0)));
            // move default entry to top if it exists
            if (previousIds.contains(PermissionsInfo.DEFAULT_ID)) {
               List<String> defaultEntry = updatedRecords.remove(updatedRecords.size() - 1);
               updatedRecords.add(0, defaultEntry);
            }
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
