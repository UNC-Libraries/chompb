package edu.unc.lib.boxc.migration.cdm.services;

import static org.slf4j.LoggerFactory.getLogger;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import edu.unc.lib.boxc.migration.cdm.options.GroupMappingSyncOptions;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.exceptions.StateAlreadyExistsException;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.GroupMappingInfo;
import edu.unc.lib.boxc.migration.cdm.model.GroupMappingInfo.GroupMapping;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo;
import edu.unc.lib.boxc.migration.cdm.options.GroupMappingOptions;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;

/**
 * Service for generating and retrieving mappings of objects to works for grouping purposes
 *
 * @author bbpennel
 */
public class GroupMappingService {
    private static final Logger log = getLogger(GroupMappingService.class);

    private static final int FETCH_SIZE = 1000;

    private MigrationProject project;
    private CdmIndexService indexService;
    private CdmFieldService fieldService;
    private List<String> exportFields;

    public void generateMapping(GroupMappingOptions options) throws Exception {
        assertProjectStateValid();
        ensureMappingState(options);

        Path mappingPath = project.getGroupMappingPath();
        boolean needsMerge = false;
        if (options.getUpdate() && Files.exists(mappingPath)) {
            mappingPath = mappingPath.getParent().resolve("~" + mappingPath.getFileName().toString() + "_new");
            // Cleanup temp path if it already exists
            Files.deleteIfExists(mappingPath);
            needsMerge = true;
        }

        Map<String, String> matchedToGroup = new HashMap<>();
        // Iterate through exported objects in this collection to match against
        Connection conn = null;
        try (
            // Write to system.out if doing a dry run, otherwise write to mappings file
            BufferedWriter writer = (options.getDryRun() && !needsMerge) ?
                    new BufferedWriter(new OutputStreamWriter(System.out)) :
                    Files.newBufferedWriter(mappingPath);
            CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(GroupMappingInfo.CSV_HEADERS));
        ) {
            // Query for all values of the export field to be used for grouping
            conn = indexService.openDbConnection();
            Statement stmt = conn.createStatement();
            stmt.setFetchSize(FETCH_SIZE);

            // Return set of all group keys that have at least 2 records in them
            var multiMemberGroupSet = new HashSet<String>();
            generateMultipleGroupMapping(options, stmt, multiMemberGroupSet, csvPrinter);
        } catch (SQLException e) {
            throw new MigrationException("Error interacting with export index", e);
        } finally {
            CdmIndexService.closeDbConnection(conn);
        }

        // Performing update operation with existing mapping, need to merge values
        if (needsMerge) {
            mergeUpdates(options, mappingPath);
        }

        if (!options.getDryRun()) {
            setUpdatedDate(Instant.now());
        }
    }

    private void generateMultipleGroupMapping(GroupMappingOptions options, Statement stmt,
                                              Set<String> multiMemberGroupSet, CSVPrinter csvPrinter) throws Exception {
        int numberGroups = options.getGroupFields().size();
        String multipleGroups = String.join(", ", options.getGroupFields());

        ResultSet groupRs = stmt.executeQuery("select " + multipleGroups
                + " from " + CdmIndexService.TB_NAME
                + " where " + CdmIndexService.ENTRY_TYPE_FIELD + " is null"
                + " group by " + multipleGroups
                + " having count(*) > 1");
        while (groupRs.next()) {
            List<String> groupValues = new ArrayList<>();
            for (int i = 1; i < numberGroups + 1; i++) {
                var groupValue = groupRs.getString(i);
                if (StringUtils.isBlank(groupValue)) {
                    continue;
                }
                groupValues.add(groupValue);
            }
            if (!groupValues.isEmpty()) {
                var multipleGroupValues = String.join(",", groupValues);
                multiMemberGroupSet.add(multipleGroupValues);
            }
        }

        ResultSet rs = stmt.executeQuery("select " + CdmFieldInfo.CDM_ID + ", " + multipleGroups
                + " from " + CdmIndexService.TB_NAME
                + " where " + CdmIndexService.ENTRY_TYPE_FIELD + " is null"
                + " order by " + CdmFieldInfo.CDM_ID + " ASC");
        while (rs.next()) {
            String cdmId = rs.getString(1);
            List<String> matchedValues = new ArrayList<>();
            for (int i = 2; i < numberGroups + 2; i++) {
                var matchedValue = rs.getString(i);
                if (!StringUtils.isBlank(matchedValue)) {
                    matchedValues.add(matchedValue);
                }
            }
            // Join matched values when grouping by multiple fields
            String multipleMatchedValues = null;
            if (numberGroups > 1 && !matchedValues.isEmpty()) {
                multipleMatchedValues = String.join(",", matchedValues);
            }

            // Add empty mapping for records either not in groups or in groups with fewer than 2 members
            if (matchedValues.isEmpty() || (numberGroups == 1 && !multiMemberGroupSet.containsAll(matchedValues))
                    || (numberGroups >= 2 && !multiMemberGroupSet.contains(multipleMatchedValues))) {
                log.debug("No matching field for object {}", cdmId);
                csvPrinter.printRecord(cdmId, null);
                continue;
            }

            List<String> listGroups = new ArrayList<>();
            for (int i = 0; i < numberGroups; i++) {
                listGroups.add(options.getGroupFields().get(i) + ":" + matchedValues.get(i));
            }
            String groupKey = GroupMappingInfo.GROUPED_WORK_PREFIX + String.join(",", listGroups);
            csvPrinter.printRecord(cdmId, groupKey);
        }
    }

    private void assertProjectStateValid() {
        if (project.getProjectProperties().getIndexedDate() == null) {
            throw new InvalidProjectStateException("Project must be indexed prior to generating source mappings");
        }
    }

    private void ensureMappingState(GroupMappingOptions options) {
        if (options.getDryRun() || options.getUpdate()) {
            return;
        }
        Path mappingPath = project.getGroupMappingPath();
        if (Files.exists(mappingPath)) {
            if (options.getForce()) {
                try {
                    try {
                        Files.delete(mappingPath);
                    } catch (NoSuchFileException e) {
                        log.debug("File does not exist, skipping deletion");
                    }
                    // Clear date property in case it was set
                    setUpdatedDate(null);
                } catch (IOException e) {
                    throw new MigrationException("Failed to overwrite mapping file", e);
                }
            } else {
                throw new StateAlreadyExistsException("Cannot create mapping, a file already exists."
                        + " Use the force flag to overwrite.");
            }
        }
    }

    /**
     * Merge existing mappings with updated mappings, writing to temporary files as intermediates
     * @param options
     * @param updatesPath
     */
    private void mergeUpdates(GroupMappingOptions options, Path updatesPath) throws IOException {
        Path originalPath = project.getGroupMappingPath();
        Path mergedPath = originalPath.getParent().resolve("~" + originalPath.getFileName().toString() + "_merged");
        // Cleanup temp merged path if it already exists
        Files.deleteIfExists(mergedPath);

        // Load the new mappings into memory
        GroupMappingInfo updateInfo = loadMappings(updatesPath);

        // Iterate through the existing mappings, merging in the updated mappings when appropriate
        try (
            Reader reader = Files.newBufferedReader(originalPath);
            CSVParser originalParser = new CSVParser(reader, CSVFormat.DEFAULT
                    .withFirstRecordAsHeader()
                    .withHeader(SourceFilesInfo.CSV_HEADERS)
                    .withTrim());
            // Write to system.out if doing a dry run, otherwise write to mappings file
            BufferedWriter writer = options.getDryRun() ?
                    new BufferedWriter(new OutputStreamWriter(System.out)) :
                    Files.newBufferedWriter(mergedPath);
            CSVPrinter mergedPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT
                    .withHeader(SourceFilesInfo.CSV_HEADERS));
        ) {
            Set<String> origIds = new HashSet<>();
            for (CSVRecord originalRecord : originalParser) {
                GroupMapping origMapping = new GroupMapping();
                origMapping.setCdmId(originalRecord.get(0));
                origMapping.setGroupKey(originalRecord.get(1));

                GroupMapping updateMapping = updateInfo.getMappingByCdmId(origMapping.getCdmId());
                if (updateMapping != null && updateMapping.getGroupKey() != null) {
                    if (options.getForce() || origMapping.getGroupKey() == null) {
                        // overwrite entry with updated mapping if using force or original didn't have a group
                        writeMapping(mergedPrinter, updateMapping);
                    } else {
                        // retain original data
                        writeMapping(mergedPrinter, origMapping);
                    }
                } else {
                    // No updates or change, retain original
                    writeMapping(mergedPrinter, origMapping);
                }
                origIds.add(originalRecord.get(0));
            }

            // Add in any records that were updated but not present in the original document
            Set<String> updateIds = updateInfo.getMappings().stream()
                    .map(GroupMapping::getCdmId).collect(Collectors.toSet());
            updateIds.removeAll(origIds);
            for (String id : updateIds) {
                writeMapping(mergedPrinter, updateInfo.getMappingByCdmId(id));
            }
        }

        // swap the merged mappings to be the main mappings, unless we're doing a dry run
        if (!options.getDryRun()) {
            Files.move(mergedPath, originalPath, StandardCopyOption.REPLACE_EXISTING);
        }
        Files.delete(updatesPath);
    }

    private void writeMapping(CSVPrinter csvPrinter, GroupMapping mapping) throws IOException {
        csvPrinter.printRecord(mapping.getCdmId(), mapping.getGroupKey());
    }

    protected void setUpdatedDate(Instant timestamp) throws IOException {
        project.getProjectProperties().setGroupMappingsUpdatedDate(timestamp);
        ProjectPropertiesSerialization.write(project);
    }

    public GroupMappingInfo loadMappings() throws IOException {
        return loadMappings(project.getGroupMappingPath());
    }

    /**
     * @return the group mapping info for the provided project
     * @throws IOException
     */
    public GroupMappingInfo loadMappings(Path mappingPath) throws IOException {
        try (
            Reader reader = Files.newBufferedReader(mappingPath);
            CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                    .withFirstRecordAsHeader()
                    .withHeader(GroupMappingInfo.CSV_HEADERS)
                    .withTrim());
        ) {
            GroupMappingInfo info = new GroupMappingInfo();
            List<GroupMapping> mappings = new ArrayList<>();
            Map<String, List<String>> grouped = info.getGroupedMappings();
            info.setMappings(mappings);
            info.setGroupedMappings(grouped);
            for (CSVRecord csvRecord : csvParser) {
                String id = csvRecord.get(0);
                String groupKey = csvRecord.get(1);
                GroupMapping mapping = new GroupMapping();
                mapping.setCdmId(id);
                mapping.setGroupKey(groupKey);
                mappings.add(mapping);
                if (StringUtils.isBlank(groupKey)) {
                    continue;
                }
                List<String> ids = grouped.computeIfAbsent(groupKey, v -> new ArrayList<>());
                ids.add(id);
            }
            return info;
        }
    }

    /**
     * Syncs group mappings from the mapping file into the database. Clears out any previously synced
     * group mapping details before updating.
     * @throws IOException
     */
    public void syncMappings(GroupMappingSyncOptions options) throws IOException {
        assertProjectStateValid();
        if (project.getProjectProperties().getGroupMappingsUpdatedDate() == null
                || Files.notExists(project.getGroupMappingPath())) {
            throw new InvalidProjectStateException("Project has not previously generated group mappings");
        }
        assertSortFieldValid(options);

        Connection conn = null;
        try {
            conn = indexService.openDbConnection();
            Statement stmt = conn.createStatement();
            // Cleanup any previously synced grouping data
            cleanupStaleSyncedGroups(stmt);
            if (project.getProjectProperties().getGroupMappingsSyncedDate() != null) {
                setSyncedDate(null);
            }

            // Sync the grouping data and generated works into the database
            GroupMappingInfo info = loadMappings();
            for (Entry<String, List<String>> groupEntry : info.getGroupedMappings().entrySet()) {
                String groupId = groupEntry.getKey();
                var childrenIds = groupEntry.getValue();
                // Do no sync groups which only contain one child
                if (childrenIds.size() <= 1) {
                    continue;
                }

                createGroupedWorkEntry(stmt, groupId, childrenIds);
                assignChildrenToGroups(stmt, groupId, childrenIds);
                updateGroupedChildrenOrder(stmt, options, groupEntry.getKey(), groupEntry.getValue());
            }
        } catch (SQLException e) {
            throw new MigrationException("Error interacting with export index", e);
        } finally {
            CdmIndexService.closeDbConnection(conn);
        }
        setSyncedDate(Instant.now());
    }

    private void assertSortFieldValid(GroupMappingSyncOptions options) {
        if (StringUtils.isBlank(options.getSortField())) {
            throw new IllegalArgumentException("Sort field must be provided");
        }
        if (!getExportFields().contains(options.getSortField())) {
            throw new IllegalArgumentException("Sort field must be a valid field for this project. Field '"
                    + options.getSortField() + "' was provided, but valid project fields are: "
                    + String.join(", ", getExportFields()));
        }
    }

    private void cleanupStaleSyncedGroups(Statement stmt) throws SQLException {
        // Clear out of date parent ids
        stmt.executeUpdate("update " + CdmIndexService.TB_NAME
                + " set " + CdmIndexService.PARENT_ID_FIELD + " = null,"
                + CdmIndexService.CHILD_ORDER_FIELD + " = null"
                + " where " + CdmIndexService.PARENT_ID_FIELD
                + " like '" + GroupMappingInfo.GROUPED_WORK_PREFIX + "%'");
        // Clear out of date generated grouping works
        stmt.executeUpdate("delete from " + CdmIndexService.TB_NAME
                + " where " + CdmIndexService.ENTRY_TYPE_FIELD
                + " = '" + CdmIndexService.ENTRY_TYPE_GROUPED_WORK + "'");
    }

    private void createGroupedWorkEntry(Statement stmt, String groupId, List<String> childrenIds) throws SQLException {
        var joinedFields = "\"" + String.join("\",\"", getExportFields()) + "\"";
        // Clone the first child's data as the base data for the new work
        String firstChild = childrenIds.get(0);
        stmt.executeUpdate("insert into " + CdmIndexService.TB_NAME
                + " (" + joinedFields + ","
                + CdmFieldInfo.CDM_ID + "," + CdmIndexService.ENTRY_TYPE_FIELD + ")"
                + " select " + joinedFields
                + ",'" + groupId + "','" + CdmIndexService.ENTRY_TYPE_GROUPED_WORK + "'"
                + " from " + CdmIndexService.TB_NAME
                + " where " + CdmFieldInfo.CDM_ID + " = " + firstChild);
    }

    private void assignChildrenToGroups(Statement stmt, String groupId, List<String> childrenIds) throws SQLException {
        // Set the parent id for the children
        for (String childId : childrenIds) {
            stmt.executeUpdate("update " + CdmIndexService.TB_NAME
                    + " set " + CdmIndexService.PARENT_ID_FIELD + " = '" + groupId + "'"
                    + " where " + CdmFieldInfo.CDM_ID + " = '"  + childId + "'");
        }
    }

    private void updateGroupedChildrenOrder(Statement stmt, GroupMappingSyncOptions options,
                                            String groupId, List<String> childrenIds) throws SQLException {
        // Retrieve list of children cdm_ids ordered by the sort field with the current group
        ResultSet rs = stmt.executeQuery("select " + CdmFieldInfo.CDM_ID
                + " from " + CdmIndexService.TB_NAME
                + " where " + CdmIndexService.PARENT_ID_FIELD + " = '" + groupId + "'"
                + " order by " + options.getSortField() + " ASC");
        var orderList = new ArrayList<String>();
        while (rs.next()) {
            orderList.add(rs.getString(1));
        }
        // Update each child to assign an order id based on its index within the group when sorted by the sort field
        for (String childId : childrenIds) {
            var orderId = orderList.indexOf(childId);
            stmt.executeUpdate("update " + CdmIndexService.TB_NAME
                    + " set " + CdmIndexService.CHILD_ORDER_FIELD + " = '" + orderId + "'"
                    + " where " + CdmFieldInfo.CDM_ID + " = '"  + childId + "'");
        }
    }

    private void setSyncedDate(Instant timestamp) throws IOException {
        project.getProjectProperties().setGroupMappingsSyncedDate(timestamp);
        ProjectPropertiesSerialization.write(project);
    }

    private List<String> getExportFields() {
        if (exportFields == null) {
            CdmFieldInfo fieldInfo = null;
            fieldInfo = fieldService.loadFieldsFromProject(project);
            exportFields = new ArrayList<>(fieldInfo.listAllExportFields());
            exportFields.remove(CdmFieldInfo.CDM_ID);
        }
        return exportFields;
    }

    public void setProject(MigrationProject project) {
        this.project = project;
    }

    public void setIndexService(CdmIndexService indexService) {
        this.indexService = indexService;
    }

    public void setFieldService(CdmFieldService fieldService) {
        this.fieldService = fieldService;
    }
}
