package edu.unc.lib.boxc.migration.cdm;

import edu.unc.lib.boxc.migration.cdm.model.AspaceRefIdInfo;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProjectProperties;
import edu.unc.lib.boxc.migration.cdm.services.CdmIndexService;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AspaceRefIdCommandIT extends AbstractCommandIT {
    private Path basePath;

    @BeforeEach
    public void setup() throws Exception {
        initProjectAndHelper();
        basePath = tmpFolder;
    }

    @Test
    public void generateNotIndexedTest() throws Exception {
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "aspace_ref_id", "generate"};
        executeExpectFailure(args);

        assertOutputContains("Project must be indexed");

        assertUpdatedDateNotPresent();
    }

    @Test
    public void generateAspaceRefIdMappingSucceedsTest() throws Exception {
        indexExportSamples();
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "aspace_ref_id", "generate"};
        executeExpectSuccess(args);

        assertTrue(Files.exists(project.getAspaceRefIdMappingPath()));

        assertUpdatedDatePresent();
    }

    @Test
    public void generateAndSyncAspaceRefITest() throws Exception {
        testHelper.indexExportData("mini_gilmer");

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "aspace_ref_id", "generate"};
        executeExpectSuccess(args);

        assertTrue(Files.exists(project.getAspaceRefIdMappingPath()));
        assertUpdatedDatePresent();

        writeCsv(mappingBody("25,2817ec3c77e5ea9846d5c070d58d402b", "26,3817ec3c77e5ea9846d5c070d58d402b",
                "27,4817ec3c77e5ea9846d5c070d58d402b"));

        String[] args2 = new String[] {
                "-w", project.getProjectPath().toString(),
                "aspace_ref_id", "sync" };
        executeExpectSuccess(args2);

        var indexService = new CdmIndexService();
        indexService.setProject(project);
        Connection conn = null;
        try {
            conn = indexService.openDbConnection();
            assertWorkSynced(conn, "25", "2817ec3c77e5ea9846d5c070d58d402b");
            assertWorkSynced(conn, "26", "3817ec3c77e5ea9846d5c070d58d402b");
            assertWorkSynced(conn, "27", "4817ec3c77e5ea9846d5c070d58d402b");
            assertSyncedDatePresent();
        } finally {
            CdmIndexService.closeDbConnection(conn);
        }
    }

    private void indexExportSamples() throws Exception {
        testHelper.indexExportData("mini_gilmer");
    }

    private void assertWorkSynced(Connection conn, String expectedCdmId, String expectedAspaceRefId)
            throws Exception {
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select * from " + CdmIndexService.TB_NAME
                + " where " + CdmFieldInfo.CDM_ID + " = " + expectedCdmId);
        while (rs.next()) {
            String cdmId = rs.getString(CdmFieldInfo.CDM_ID);
            String refId = rs.getString(CdmIndexService.ASPACE_REF_ID);
            assertEquals(expectedCdmId, cdmId);
            assertEquals(expectedAspaceRefId, refId);
        }
    }

    private void assertUpdatedDatePresent() throws Exception {
        MigrationProjectProperties props = ProjectPropertiesSerialization.read(project.getProjectPropertiesPath());
        assertNotNull(props.getAspaceRefIdMappingsUpdatedDate(), "Updated timestamp must be set");
    }

    private void assertUpdatedDateNotPresent() throws Exception {
        MigrationProjectProperties props = ProjectPropertiesSerialization.read(project.getProjectPropertiesPath());
        assertNull(props.getAspaceRefIdMappingsUpdatedDate(), "Updated timestamp must not be set");
    }

    private void assertSyncedDatePresent() throws Exception {
        MigrationProjectProperties props = ProjectPropertiesSerialization.read(project.getProjectPropertiesPath());
        assertNotNull(props.getAspaceRefIdMappingsSyncedDate());
    }

    private String mappingBody(String... rows) {
        return String.join(",", AspaceRefIdInfo.CSV_HEADERS) + "\n"
                + String.join("\n", rows);
    }

    private void writeCsv(String mappingBody) throws IOException {
        FileUtils.write(project.getAspaceRefIdMappingPath().toFile(),
                mappingBody, StandardCharsets.UTF_8);
        project.getProjectProperties().setAspaceRefIdMappingsUpdatedDate(Instant.now());
        ProjectPropertiesSerialization.write(project);
    }
}
