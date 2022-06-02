/**
 * Copyright 2008 The University of North Carolina at Chapel Hill
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.unc.lib.boxc.migration.cdm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.unc.lib.boxc.migration.cdm.services.CdmFileRetrievalService;
import edu.unc.lib.boxc.migration.cdm.test.SipServiceHelper;
import org.junit.Before;
import org.junit.Test;

import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.CdmIndexService;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;

/**
 * @author bbpennel
 */
public class GroupMappingCommandIT extends AbstractCommandIT {
    private final static String COLLECTION_ID = "my_coll";

    private MigrationProject project;
    private CdmIndexService indexService;
    private SipServiceHelper testHelper;

    @Before
    public void setup() throws Exception {
        project = MigrationProjectFactory.createMigrationProject(
                baseDir, COLLECTION_ID, null, USERNAME);
        testHelper = new SipServiceHelper(project, tmpFolder.getRoot().toPath());
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

        indexService = new CdmIndexService();
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
        assertTrue("Expected parent ids set for file objects " + expected
                + " but only was " + childIds, childIds.containsAll(expected));
        assertEquals(expected.size(), childIds.size());
    }

    private void indexExportSamples() throws Exception {
        testHelper.indexExportData("mini_gilmer");
    }
}
