package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.exceptions.StateAlreadyExistsException;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.GroupMappingInfo;
import edu.unc.lib.boxc.migration.cdm.model.GroupMappingInfo.GroupMapping;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProjectProperties;
import edu.unc.lib.boxc.migration.cdm.options.CdmIndexOptions;
import edu.unc.lib.boxc.migration.cdm.options.GroupMappingOptions;
import edu.unc.lib.boxc.migration.cdm.options.GroupMappingSyncOptions;
import edu.unc.lib.boxc.migration.cdm.test.BxcEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.OutputHelper;
import edu.unc.lib.boxc.migration.cdm.test.SipServiceHelper;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static edu.unc.lib.boxc.migration.cdm.services.CdmFieldService.CSV;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author bbpennel
 */
public class GroupMappingServiceTest {
    private static final String PROJECT_NAME = "proj";

    @TempDir
    public Path tmpFolder;

    private MigrationProject project;
    private CdmIndexService cdmIndexService;
    private CdmFieldService fieldService;
    private FileIndexService fileIndexService;
    private GroupMappingService service;
    private SipServiceHelper testHelper;

    @BeforeEach
    public void setup() throws Exception {
        project = MigrationProjectFactory.createCdmMigrationProject(
                tmpFolder, PROJECT_NAME, null, "user",
                CdmEnvironmentHelper.DEFAULT_ENV_ID, BxcEnvironmentHelper.DEFAULT_ENV_ID);
        Files.createDirectories(project.getExportPath());

        fieldService = new CdmFieldService();
        cdmIndexService = new CdmIndexService();
        cdmIndexService.setProject(project);
        cdmIndexService.setFieldService(fieldService);
        fileIndexService = new FileIndexService();
        fileIndexService.setSource(CSV);
        fileIndexService.setProject(project);
        fileIndexService.setFieldService(fieldService);
        service = new GroupMappingService();
        service.setIndexService(cdmIndexService);
        service.setProject(project);
        service.setFieldService(fieldService);

        testHelper = new SipServiceHelper(project, tmpFolder);
    }

    @Test
    public void generateNoIndexTest() throws Exception {
        GroupMappingOptions options = makeDefaultOptions();

        var e = assertThrows(InvalidProjectStateException.class, () -> {
            service.generateMapping(options);
            fail();
        });
        assertExceptionContains("Project must be indexed", e);
        assertMappedDateNotPresent();
    }

    @Test
    public void generateSingleRunTest() throws Exception {
        indexExportSamples();
        GroupMappingOptions options = makeDefaultOptions();
        service.generateMapping(options);

        GroupMappingInfo info = service.loadMappings();
        assertGroupAMappingsPresent(info);
        assertMappedDatePresent();
    }

    @Test
    public void generateDryRunTest() throws Exception {
        OutputHelper.captureOutput(() -> {
            assertThrows(NoSuchFileException.class, () -> {
                indexExportSamples();
                GroupMappingOptions options = makeDefaultOptions();
                options.setDryRun(true);
                service.generateMapping(options);

                service.loadMappings();
                fail();
            });
        });

        assertMappedDateNotPresent();
    }

    @Test
    public void generateDoubleRunTest() throws Exception {
        indexExportSamples();
        GroupMappingOptions options = makeDefaultOptions();
        service.generateMapping(options);

        assertThrows(StateAlreadyExistsException.class, () -> {
            service.generateMapping(options);
        });

        // mapping state should be unchanged
        GroupMappingInfo info = service.loadMappings();
        assertGroupAMappingsPresent(info);

        assertMappedDatePresent();
    }

    @Test
    public void generateSecondDryRunTest() throws Exception {
        OutputHelper.captureOutput(() -> {
            indexExportSamples();
            GroupMappingOptions options = makeDefaultOptions();
            service.generateMapping(options);
            options.setDryRun(true);

            service.generateMapping(options);
            // mapping state should be unchanged
            GroupMappingInfo info = service.loadMappings();
            assertGroupAMappingsPresent(info);
            assertMappedDatePresent();
        });
    }

    @Test
    public void generateForceRunTest() throws Exception {
        indexExportSamples();
        GroupMappingOptions options = makeDefaultOptions();
        service.generateMapping(options);

        GroupMappingInfo info = service.loadMappings();
        assertGroupAMappingsPresent(info);

        options.setForce(true);
        options.setGroupFields(Arrays.asList("digitc"));

        service.generateMapping(options);

        // Mapping state should have been overwritten
        GroupMappingInfo info2 = service.loadMappings();
        assertMappingPresent(info2, "25", "digitc:2005-11-10");
        assertMappingPresent(info2, "26", null);
        assertMappingPresent(info2, "27", null);
        assertMappingPresent(info2, "28", "digitc:2005-11-10");
        assertMappingPresent(info2, "29", "digitc:2005-11-10");
        assertEquals(5, info2.getMappings().size());

        assertGroupingPresent(info2, "digitc:2005-11-10", "25", "28", "29");
        assertEquals(1, info2.getGroupedMappings().size());

        assertMappedDatePresent();
    }

    @Test
    public void generateUpdateRunTest() throws Exception {
        indexExportSamples();
        GroupMappingOptions options = makeDefaultOptions();
        service.generateMapping(options);

        GroupMappingInfo info = service.loadMappings();
        assertEquals(5, info.getMappings().size());

        options.setUpdate(true);
        options.setGroupFields(Arrays.asList("digitc"));

        service.generateMapping(options);

        // Mapping state should have been overwritten
        GroupMappingInfo info2 = service.loadMappings();
        assertMappingPresent(info2, "25", "groupa:group1");
        assertMappingPresent(info2, "26", "groupa:group1");
        assertMappingPresent(info2, "27", null);
        assertMappingPresent(info2, "28", "digitc:2005-11-10");
        assertMappingPresent(info2, "29", "digitc:2005-11-10");
        assertEquals(5, info2.getMappings().size());

        assertMappedDatePresent();
    }

    @Test
    public void generateUpdateForceRunTest() throws Exception {
        indexExportSamples();
        GroupMappingOptions options = makeDefaultOptions();
        options.setGroupFields(Arrays.asList("digitc"));
        service.generateMapping(options);

        GroupMappingInfo info = service.loadMappings();
        assertMappingPresent(info, "25", "digitc:2005-11-10");
        assertMappingPresent(info, "26", null);
        assertMappingPresent(info, "27", null);
        assertMappingPresent(info, "28", "digitc:2005-11-10");
        assertMappingPresent(info, "29", "digitc:2005-11-10");
        assertEquals(5, info.getMappings().size());

        options.setUpdate(true);
        options.setForce(true);
        options.setGroupFields(Arrays.asList("groupa"));
        service.generateMapping(options);

        GroupMappingInfo info2 = service.loadMappings();
        assertMappingPresent(info2, "25", "groupa:group1");
        assertMappingPresent(info2, "26", "groupa:group1");
        assertMappingPresent(info2, "27", null);
        assertMappingPresent(info2, "28", "digitc:2005-11-10");
        assertMappingPresent(info2, "29", "digitc:2005-11-10");
        assertEquals(5, info2.getMappings().size());

        assertMappedDatePresent();
    }

    @Test
    public void syncNotIndexedTest() throws Exception {
        var e = assertThrows(InvalidProjectStateException.class, () -> {
            service.syncMappings(makeDefaultSyncOptions());
        });
        assertExceptionContains("Project must be indexed", e);
        assertMappedDateNotPresent();
        assertSyncedDateNotPresent();
    }

    @Test
    public void syncNotGeneratedTest() throws Exception {
        indexExportSamples();
        var e = assertThrows(InvalidProjectStateException.class, () -> {
            service.syncMappings(makeDefaultSyncOptions());
        });
        assertExceptionContains("Project has not previously generated group mappings", e);
        assertMappedDateNotPresent();
        assertSyncedDateNotPresent();
    }

    @Test
    public void syncNoSortFieldTest() throws Exception {
        indexExportSamples();
        service.generateMapping(makeDefaultOptions());

        var e = assertThrows(IllegalArgumentException.class, () -> {
            var options = makeDefaultSyncOptions();
            options.setSortField("");
            service.syncMappings(options);
        });
        assertExceptionContains("Sort field must be provided", e);
        assertSyncedDateNotPresent();
    }

    @Test
    public void syncInvalidSortFieldTest() throws Exception {
        indexExportSamples();
        service.generateMapping(makeDefaultOptions());
        var e = assertThrows(IllegalArgumentException.class, () -> {
            var options = makeDefaultSyncOptions();
            options.setSortField("boxy");
            service.syncMappings(options);
        });
        assertExceptionContains("Sort field must be a valid field for this project", e);
        assertSyncedDateNotPresent();
    }

    @Test
    public void syncSingleRunTest() throws Exception {
        indexExportSamples();
        GroupMappingOptions options = makeDefaultOptions();
        service.generateMapping(options);

        service.syncMappings(makeDefaultSyncOptions());

        Connection conn = null;
        try {
            GroupMappingInfo info = service.loadMappings();
            conn = cdmIndexService.openDbConnection();
            assertWorkSynced(conn, "groupa:group1", "Redoubt C", "2005-11-23");
            assertFilesGrouped(conn, "groupa:group1", "25", "26");
            assertFileHasOrder(conn, "25", 1);
            assertFileHasOrder(conn, "26", 0);
            // Group key with a single child should not be grouped
            assertNumberOfGroups(conn, 1);
            assertParentIdsPresent(conn, "groupa:group1", null);
            assertSyncedDatePresent();
        } finally {
            CdmIndexService.closeDbConnection(conn);
        }
    }

    @Test
    public void syncSecondRunWithCleanupTest() throws Exception {
        indexExportSamples();
        GroupMappingOptions options = makeDefaultOptions();
        options.setGroupFields(Arrays.asList("digitc"));
        service.generateMapping(options);

        service.syncMappings(makeDefaultSyncOptions());

        Connection conn = null;
        try {
            GroupMappingInfo info = service.loadMappings();
            conn = cdmIndexService.openDbConnection();
            assertWorkSynced(conn, "digitc:2005-11-10", "Redoubt C", "2005-11-23");
            assertFilesGrouped(conn, "digitc:2005-11-10", "25", "28", "29");
            assertFileHasOrder(conn, "25", 0);
            assertFileHasOrder(conn, "28", 1);
            assertFileHasOrder(conn, "29", 2);
            assertNumberOfGroups(conn, 1);
            assertParentIdsPresent(conn, "digitc:2005-11-10", null);
            assertSyncedDatePresent();
        } finally {
            CdmIndexService.closeDbConnection(conn);
        }

        options.setGroupFields(Arrays.asList("groupa"));
        options.setForce(true);
        service.generateMapping(options);

        service.syncMappings(makeDefaultSyncOptions());

        try {
            GroupMappingInfo info = service.loadMappings();
            conn = cdmIndexService.openDbConnection();
            assertWorkSynced(conn, "groupa:group1", "Redoubt C", "2005-11-23");
            assertFilesGrouped(conn, "groupa:group1", "25", "26");
            assertFileHasOrder(conn, "25", 1);
            assertFileHasOrder(conn, "26", 0);
            // Group key with a single child should not be grouped
            assertNumberOfGroups(conn, 1);
            assertParentIdsPresent(conn, "groupa:group1", null);

            assertGroupingPresent(info, "groupa:group1", "25", "26");
            assertEquals(1, info.getGroupedMappings().size());

            assertSyncedDatePresent();
        } finally {
            CdmIndexService.closeDbConnection(conn);
        }
    }

    @Test
    public void generateSingleRunMultipleGroupsTest() throws Exception {
        indexExportSamples();
        GroupMappingOptions options = new GroupMappingOptions();
        options.setGroupFields(Arrays.asList("groupa", "dcmi"));
        service.generateMapping(options);

        GroupMappingInfo info = service.loadMappings();

        assertMappingPresent(info, "25", "groupa:group1,dcmi:Image");
        assertMappingPresent(info, "26", "groupa:group1,dcmi:Image");
        assertMappingPresent(info, "27", null);
        assertMappingPresent(info, "28", null);
        assertMappingPresent(info, "29", null);
        assertEquals(5, info.getMappings().size());

        assertGroupingPresent(info, "groupa:group1,dcmi:Image", "25", "26");
        assertEquals(1, info.getGroupedMappings().size());

        assertMappedDatePresent();
    }

    @Test
    public void syncSingleRunMultipleGroupsTest() throws Exception {
        indexExportSamples();
        GroupMappingOptions options = new GroupMappingOptions();
        options.setGroupFields(Arrays.asList("groupa", "dcmi"));
        service.generateMapping(options);

        service.syncMappings(makeDefaultSyncOptions());

        Connection conn = null;
        try {
            GroupMappingInfo info = service.loadMappings();
            conn = cdmIndexService.openDbConnection();
            assertWorkSynced(conn, "groupa:group1", "Redoubt C", "2005-11-23");
            assertWorkSynced(conn, "dcmi:Image", "Redoubt C", "2005-11-23");
            assertFilesGrouped(conn, "groupa:group1,dcmi:Image", "25", "26");
            assertFileHasOrder(conn, "25", 1);
            assertFileHasOrder(conn, "26", 0);
            // Group key with a single child should not be grouped
            assertNumberOfGroups(conn, 1);
            assertParentIdsPresent(conn, "groupa:group1,dcmi:Image", null);
            assertSyncedDatePresent();
        } finally {
            CdmIndexService.closeDbConnection(conn);
        }
    }

    @Test
    public void generateMultipleGroupsForceUpdateRunTest() throws Exception {
        indexExportSamples();
        GroupMappingOptions options = makeDefaultOptions();
        service.generateMapping(options);

        GroupMappingInfo info = service.loadMappings();
        assertEquals(5, info.getMappings().size());

        options.setUpdate(true);
        options.setForce(true);
        options.setGroupFields(Arrays.asList("groupa", "dcmi"));

        service.generateMapping(options);

        // Mapping state should have been overwritten
        GroupMappingInfo info2 = service.loadMappings();
        assertMappingPresent(info2, "25", "groupa:group1,dcmi:Image");
        assertMappingPresent(info2, "26", "groupa:group1,dcmi:Image");
        assertMappingPresent(info2, "27", null);
        assertMappingPresent(info2, "28", null);
        assertMappingPresent(info2, "29", null);
        assertEquals(5, info2.getMappings().size());

        assertMappedDatePresent();
    }

    @Test
    public void syncSecondRunWithCleanupMultipleGroupsTest() throws Exception {
        indexExportSamples();
        GroupMappingOptions options = makeDefaultOptions();
        options.setGroupFields(Arrays.asList("digitc"));
        service.generateMapping(options);

        service.syncMappings(makeDefaultSyncOptions());

        Connection conn = null;
        try {
            GroupMappingInfo info = service.loadMappings();
            conn = cdmIndexService.openDbConnection();
            assertWorkSynced(conn, "digitc:2005-11-10", "Redoubt C", "2005-11-23");
            assertFilesGrouped(conn, "digitc:2005-11-10", "25", "28", "29");
            assertFileHasOrder(conn, "25", 0);
            assertFileHasOrder(conn, "28", 1);
            assertFileHasOrder(conn, "29", 2);
            assertNumberOfGroups(conn, 1);
            assertParentIdsPresent(conn, "digitc:2005-11-10", null);
            assertSyncedDatePresent();
        } finally {
            CdmIndexService.closeDbConnection(conn);
        }

        options.setGroupFields(Arrays.asList("groupa", "dcmi"));
        options.setForce(true);
        service.generateMapping(options);

        service.syncMappings(makeDefaultSyncOptions());

        try {
            GroupMappingInfo info = service.loadMappings();
            conn = cdmIndexService.openDbConnection();
            assertWorkSynced(conn, "groupa:group1", "Redoubt C", "2005-11-23");
            assertWorkSynced(conn, "dcmi:Image", "Redoubt C", "2005-11-23");
            assertFilesGrouped(conn, "groupa:group1,dcmi:Image", "25", "26");
            assertFileHasOrder(conn, "25", 1);
            assertFileHasOrder(conn, "26", 0);
            // Group key with a single child should not be grouped
            assertNumberOfGroups(conn, 1);
            assertParentIdsPresent(conn, "groupa:group1,dcmi:Image", null);

            assertGroupingPresent(info, "groupa:group1,dcmi:Image", "25", "26");
            assertEquals(1, info.getGroupedMappings().size());

            assertSyncedDatePresent();
        } finally {
            CdmIndexService.closeDbConnection(conn);
        }
    }

    @Test
    public void syncGroupsFromFilesystemTest() throws Exception {
        indexFromCsv(Path.of("src/test/resources/files/more_fields.csv"));

        GroupMappingOptions options = new GroupMappingOptions();
        options.setGroupFields(Arrays.asList("file_type"));
        service.generateMapping(options);

        var syncOptions = new GroupMappingSyncOptions();
        syncOptions.setSortField("filename");
        service.syncMappings(syncOptions);

        Connection conn = null;
        try {
            GroupMappingInfo info = service.loadMappings();
            conn = cdmIndexService.openDbConnection();
            assertFilesGrouped(conn, "file_type:tif", "test-00001", "test-00002");
            assertFileHasOrder(conn, "test-00001", 0);
            assertFileHasOrder(conn, "test-00002", 1);
            // Group key with a single child should not be grouped
            assertNumberOfGroups(conn, 1);
            assertParentIdsPresent(conn, "file_type:tif", null);
            assertSyncedDatePresent();
        } finally {
            CdmIndexService.closeDbConnection(conn);
        }
    }

    private void assertWorkSynced(Connection conn, String workId, String expectedTitle, String expectedCreated)
            throws Exception {
        String groupKey = asGroupKey(workId);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select * from " + CdmIndexService.TB_NAME
                + " where " + CdmFieldInfo.CDM_ID + " = '" + groupKey + "'");
        while (rs.next()) {
            String cdmTitle = rs.getString("title");
            String cdmCreated = rs.getString(CdmFieldInfo.CDM_CREATED);
            assertEquals(expectedTitle, cdmTitle);
            assertEquals(expectedCreated, cdmCreated);
            assertEquals(CdmIndexService.ENTRY_TYPE_GROUPED_WORK, rs.getString(CdmIndexService.ENTRY_TYPE_FIELD));
            assertNull(rs.getString(CdmIndexService.PARENT_ID_FIELD));
        }
    }

    private void assertFilesGrouped(Connection conn, String workId, String... expectedFileCdmIds)
            throws Exception {
        String groupKey = asGroupKey(workId);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select " + CdmFieldInfo.CDM_ID
                + " from " + CdmIndexService.TB_NAME
                + " where " + CdmIndexService.PARENT_ID_FIELD + " = '" + groupKey + "'");
        List<String> childIds = new ArrayList<>();
        while (rs.next()) {
            childIds.add(rs.getString(1));
        }
        List<String> expected = Arrays.asList(expectedFileCdmIds);
        assertTrue(childIds.containsAll(expected), "Expected work " + workId + " to contain children " + expected
                        + " but it contained " + childIds);
        assertEquals(expected.size(), childIds.size());
    }

    private void assertFileHasOrder(Connection conn, String childId, int expectedChildOrder) throws Exception {
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select " + CdmIndexService.CHILD_ORDER_FIELD
                + " from " + CdmIndexService.TB_NAME
                + " where " + CdmFieldInfo.CDM_ID + " = '" + childId + "'");
        if (rs.next()) {
            var childOrder = rs.getInt(1);
            assertEquals(expectedChildOrder, childOrder, "Child " + childId + " did not have expected order value");
        } else {
            fail("No entry found for child id " + childId);
        }
    }

    private void assertNumberOfGroups(Connection conn, int expected) throws Exception {
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select count(distinct " + CdmFieldInfo.CDM_ID + ")"
                + " from " + CdmIndexService.TB_NAME
                + " where " + CdmIndexService.ENTRY_TYPE_FIELD
                    + " = '" + CdmIndexService.ENTRY_TYPE_GROUPED_WORK + "'");
        while (rs.next()) {
            assertEquals(expected, rs.getInt(1));
        }
    }

    private void assertParentIdsPresent(Connection conn, String... expectedParents) throws Exception {
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select distinct " + CdmIndexService.PARENT_ID_FIELD
                + " from " + CdmIndexService.TB_NAME);
        List<String> parentIds = new ArrayList<>();
        while (rs.next()) {
            parentIds.add(rs.getString(1));
        }
        var expected = Arrays.stream(expectedParents).map(this::asGroupKey).collect(Collectors.toList());
        assertTrue(parentIds.containsAll(expected), "Expected parent ids " + expected + " but found " + parentIds);
        assertEquals(expected.size(), parentIds.size(), "Expected parent ids " + expected + " but found " + parentIds);
    }

    /**
     * @param matchValue
     * @return the provided match value (fieldname+value) with the group prefix added, which is the expected
     *   format for group keys, or null if the value is null
     */
    private String asGroupKey(String matchValue) {
        if (matchValue == null) {
            return null;
        } else {
            return GroupMappingInfo.GROUPED_WORK_PREFIX + matchValue;
        }
    }

    private GroupMappingOptions makeDefaultOptions() {
        GroupMappingOptions options = new GroupMappingOptions();
        options.setGroupFields(Arrays.asList("groupa"));
        return options;
    }

    private GroupMappingSyncOptions makeDefaultSyncOptions() {
        var options = new GroupMappingSyncOptions();
        options.setSortField("file");
        return options;
    }

    private void assertExceptionContains(String expected, Exception e) {
        assertTrue(e.getMessage().contains(expected),
                "Expected message exception to contain '" + expected + "', but was: " + e.getMessage());
    }

    private void assertMappingPresent(GroupMappingInfo info, String id, String expectedMatchedVal) {
        GroupMapping mapping = info.getMappingByCdmId(id);
        assertNotNull(mapping);
        assertEquals(id, mapping.getCdmId());
        assertEquals(asGroupKey(expectedMatchedVal), mapping.getGroupKey());
    }

    private void assertGroupingPresent(GroupMappingInfo groupedInfo, String groupKey, String... cdmIds) {
        Map<String, List<String>> groupedMappings = groupedInfo.getGroupedMappings();
        List<String> objIds = groupedMappings.get(asGroupKey(groupKey));
        List<String> expectedIds = Arrays.asList(cdmIds);
        assertTrue(objIds.containsAll(expectedIds),
                "Expected group " + groupKey + " to contain " + expectedIds + " but contained " + objIds);
        assertEquals(expectedIds.size(), objIds.size(),
                "Expected group " + groupKey + " to contain " + expectedIds + " but contained " + objIds);
    }

    private void assertMappedDatePresent() throws Exception {
        MigrationProjectProperties props = ProjectPropertiesSerialization.read(project.getProjectPropertiesPath());
        assertNotNull(props.getGroupMappingsUpdatedDate());
    }

    private void assertMappedDateNotPresent() throws Exception {
        MigrationProjectProperties props = ProjectPropertiesSerialization.read(project.getProjectPropertiesPath());
        assertNull(props.getGroupMappingsUpdatedDate());
    }

    private void assertSyncedDatePresent() throws Exception {
        MigrationProjectProperties props = ProjectPropertiesSerialization.read(project.getProjectPropertiesPath());
        assertNotNull(props.getGroupMappingsSyncedDate());
    }

    private void assertSyncedDateNotPresent() throws Exception {
        MigrationProjectProperties props = ProjectPropertiesSerialization.read(project.getProjectPropertiesPath());
        assertNull(props.getGroupMappingsSyncedDate());
    }

    private void assertGroupAMappingsPresent(GroupMappingInfo info) {
        assertMappingPresent(info, "25", "groupa:group1");
        assertMappingPresent(info, "26", "groupa:group1");
        // Single member group should not be mapped
        assertMappingPresent(info, "27", null);
        assertMappingPresent(info, "28", null);
        assertMappingPresent(info, "29", null);
        assertEquals(5, info.getMappings().size());

        assertGroupingPresent(info, "groupa:group1", "25", "26");
        assertEquals(1, info.getGroupedMappings().size());
    }

    private void indexExportSamples() throws Exception {
        testHelper.indexExportData("grouped_gilmer");
    }

    public void indexFromCsv(Path csvPath) throws Exception {
        CdmFieldInfo csvExportFields = fieldService.retrieveFields(csvPath, CSV);
        fieldService.persistFieldsToProject(project, csvExportFields);
        project.getProjectProperties().setExportedDate(Instant.now());

        CdmIndexOptions options = new CdmIndexOptions();
        options.setCsvFile(csvPath);
        options.setForce(false);

        fileIndexService.createDatabase(options);
        fileIndexService.indexAllFromFile(options);
        ProjectPropertiesSerialization.write(project);
    }
}
