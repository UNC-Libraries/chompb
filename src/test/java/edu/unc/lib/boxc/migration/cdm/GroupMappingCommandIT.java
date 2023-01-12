package edu.unc.lib.boxc.migration.cdm;

import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.services.CdmIndexService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author bbpennel
 */
public class GroupMappingCommandIT extends AbstractCommandIT {
    @BeforeEach
    public void setup() throws Exception {
        initProjectAndHelper();
    }

    @Test
    public void generateNotIndexedTest() throws Exception {
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "group_mapping", "generate" };
        executeExpectFailure(args);

        assertOutputContains("Project must be indexed");
    }

    @Test
    public void generateBasicMatchSucceedsTest() throws Exception {
        indexExportSamples();
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "group_mapping", "generate",
                "-n", "groupa"};
        executeExpectSuccess(args);

        assertTrue(Files.exists(project.getGroupMappingPath()));
    }

    @Test
    public void generateAndSyncTest() throws Exception {
        indexExportSamples();
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "group_mapping", "generate",
                "-n", "groupa" };
        executeExpectSuccess(args);

        assertTrue(Files.exists(project.getGroupMappingPath()));

        String[] args2 = new String[] {
                "-w", project.getProjectPath().toString(),
                "group_mapping", "sync" };
        executeExpectSuccess(args2);

        var indexService = new CdmIndexService();
        indexService.setProject(project);
        Connection conn = null;
        try {
            conn = indexService.openDbConnection();
            assertFilesGrouped(conn, "25", "26");
        } finally {
            CdmIndexService.closeDbConnection(conn);
        }
    }

    @Test
    public void statusNotGenerated() throws Exception {
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "group_mapping", "status" };
        executeExpectSuccess(args);

        assertOutputMatches(".*Last Generated: +Not completed.*");
    }

    @Test
    public void statusGenerated() throws Exception {
        indexExportSamples();
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "group_mapping", "generate",
                "-n", "groupa"};
        executeExpectSuccess(args);

        String[] args2 = new String[] {
                "-w", project.getProjectPath().toString(),
                "group_mapping", "status" };
        executeExpectSuccess(args2);

        assertOutputMatches(".*Last Generated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Mappings Modified: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Last Synched: +Not completed.*");

        String[] args3 = new String[] {
                "-w", project.getProjectPath().toString(),
                "group_mapping", "sync" };
        executeExpectSuccess(args3);

        resetOutput();

        // Normal verbosity
        String[] args4 = new String[] {
                "-w", project.getProjectPath().toString(),
                "group_mapping", "status" };
        executeExpectSuccess(args4);

        assertOutputMatches(".*Last Generated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Mappings Modified: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Last Synched: +[0-9\\-T:]+.*");

        assertOutputMatches(".*Total Groups: +1.*");
        assertOutputMatches(".*Objects In Groups: +2.*");
        assertOutputNotMatches(".*Counts per group.*");

        resetOutput();

        // Quiet verbosity
        String[] args5 = new String[] {
                "-w", project.getProjectPath().toString(),
                "group_mapping", "status",
                "-q" };
        executeExpectSuccess(args5);

        assertOutputMatches(".*Last Generated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Mappings Modified: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Last Synched: +[0-9\\-T:]+.*");

        assertOutputNotMatches(".*Total Groups.*");

        resetOutput();

        // Verbose
        String[] args6 = new String[] {
                "-w", project.getProjectPath().toString(),
                "group_mapping", "status",
                "-v" };
        executeExpectSuccess(args6);

        assertOutputMatches(".*Last Generated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Mappings Modified: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Last Synched: +[0-9\\-T:]+.*");

        assertOutputMatches(".*Total Groups: +1.*");
        assertOutputMatches(".*Objects In Groups: +2.*");
    }

    private void assertFilesGrouped(Connection conn, String... expectedFileCdmIds)
            throws Exception {
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select " + CdmFieldInfo.CDM_ID
                + " from " + CdmIndexService.TB_NAME
                + " where " + CdmIndexService.PARENT_ID_FIELD + " is not null");
        List<String> childIds = new ArrayList<>();
        while (rs.next()) {
            childIds.add(rs.getString(1));
        }
        List<String> expected = Arrays.asList(expectedFileCdmIds);
        assertTrue(childIds.containsAll(expected), "Expected parent ids set for file objects " + expected
                        + " but only was " + childIds);
        assertEquals(expected.size(), childIds.size());
    }

    private void indexExportSamples() throws Exception {
        testHelper.indexExportData("mini_gilmer");
    }
}
