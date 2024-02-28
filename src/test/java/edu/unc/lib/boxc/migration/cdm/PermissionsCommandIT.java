package edu.unc.lib.boxc.migration.cdm;

import edu.unc.lib.boxc.migration.cdm.model.PermissionsInfo;
import edu.unc.lib.boxc.migration.cdm.options.GroupMappingOptions;
import edu.unc.lib.boxc.migration.cdm.options.GroupMappingSyncOptions;
import edu.unc.lib.boxc.migration.cdm.services.PermissionsService;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PermissionsCommandIT extends AbstractCommandIT {
    @BeforeEach
    public void setup() throws Exception {
        initProjectAndHelper();
        setupChompbConfig();
    }

    @Test
    public void generateNoDefaultPermissions() throws Exception {
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "permissions", "generate"};
        executeExpectSuccess(args);
    }

    @Test
    public void generateDefaultPermissions() throws Exception {
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "permissions", "generate",
                "-wd",
                "--everyone", "canViewMetadata",
                "--authenticated", "canViewMetadata"};
        executeExpectSuccess(args);
        assertMapping(0, "default", "canViewMetadata", "canViewMetadata");
    }

    @Test
    public void generateDefaultPermissionsUnspecified() throws Exception {
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "permissions", "generate",
                "-wd"};
        executeExpectSuccess(args);
        assertMapping(0, "default", "canViewOriginals", "canViewOriginals");
    }

    @Test
    public void generateDefaultPermissionsStaffOnly() throws Exception {
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "permissions", "generate",
                "-wd",
                "-so"};
        executeExpectSuccess(args);
        assertMapping(0, "default", "none", "none");
    }

    @Test
    public void generateDefaultPermissionsInvalid() throws Exception {
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "permissions", "generate",
                "-wd",
                "--everyone", "canViewMetadata",
                "--authenticated", "canManage"};
        executeExpectFailure(args);
        assertOutputContains("Assigned role value is invalid. Must be one of the following patron roles: " +
                "[none, canDiscover, canViewMetadata, canViewAccessCopies, canViewReducedQuality, canViewOriginals]");
    }

    @Test
    public void generateDefaultPermissionsWithoutForceFlag() throws Exception {
        FileUtils.write(project.getPermissionsPath().toFile(),
                "default,canViewMetadata,canViewMetadata", StandardCharsets.UTF_8, true);

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "permissions", "generate",
                "-wd",
                "--everyone", "canViewMetadata",
                "--authenticated", "canViewMetadata"};
        executeExpectFailure(args);
        assertOutputContains("Cannot create permissions, a file already exists. " +
                "Use the force flag to overwrite.");
    }

    @Test
    public void generateDefaultPermissionsWithForceFlag() throws Exception {
        FileUtils.write(project.getPermissionsPath().toFile(),
                "default,canViewMetadata,canViewMetadata", StandardCharsets.UTF_8, true);

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "permissions", "generate",
                "-wd",
                "--everyone", "canViewOriginals",
                "--authenticated", "canViewOriginals",
                "--force"};
        executeExpectSuccess(args);
        assertMapping(0, "default", "canViewOriginals", "canViewOriginals");
    }

    @Test
    public void generateWorkPermissions() throws Exception {
        testHelper.indexExportData("grouped_gilmer");
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "permissions", "generate",
                "-ww",
                "--everyone", "canViewOriginals",
                "--authenticated", "canViewOriginals"};
        executeExpectSuccess(args);
        assertMapping(0, "25", "canViewOriginals", "canViewOriginals");
        assertMapping(1, "26", "canViewOriginals", "canViewOriginals");
        assertMapping(2, "27", "canViewOriginals", "canViewOriginals");
    }

    @Test
    public void generateWorkPermissionsWithDefault() throws Exception {
        testHelper.indexExportData("grouped_gilmer");
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "permissions", "generate",
                "-wd",
                "-ww",
                "--everyone", "canViewMetadata",
                "--authenticated", "canViewMetadata"};
        executeExpectSuccess(args);
        assertMapping(0, "default", "canViewMetadata", "canViewMetadata");
        assertMapping(1, "25", "canViewMetadata", "canViewMetadata");
        assertMapping(2, "26", "canViewMetadata", "canViewMetadata");
        assertMapping(3, "27", "canViewMetadata", "canViewMetadata");
    }

    @Test
    public void generateFilePermissionsWithDefault() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "permissions", "generate",
                "-wd",
                "-wf",
                "-e", "canViewMetadata",
                "-a", "canViewMetadata"};
        executeExpectSuccess(args);
        assertMapping(0, "default", "canViewMetadata", "canViewMetadata");
    }

    @Test
    public void generateWorkAndFilePermissionsWithForce() throws Exception {
        FileUtils.write(project.getPermissionsPath().toFile(),
                "default,canViewOriginals,canViewOriginals", StandardCharsets.UTF_8, true);

        testHelper.indexExportData("mini_gilmer");
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "permissions", "generate",
                "-ww",
                "-wf",
                "-e", "canViewMetadata",
                "-a", "canViewMetadata",
                "-f"};
        executeExpectSuccess(args);
        assertMapping(0, "25", "canViewMetadata", "canViewMetadata");
        assertMapping(1, "26", "canViewMetadata", "canViewMetadata");
        assertMapping(2, "27", "canViewMetadata", "canViewMetadata");
    }

    @Test
    public void generateWorkAndFilePermissionsWithDefault() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "permissions", "generate",
                "-wd",
                "-ww",
                "-wf",
                "--everyone", "canViewMetadata",
                "--authenticated", "canViewMetadata"};
        executeExpectSuccess(args);
        assertMapping(0, "default", "canViewMetadata", "canViewMetadata");
        assertMapping(1, "25", "canViewMetadata", "canViewMetadata");
        assertMapping(2, "26", "canViewMetadata", "canViewMetadata");
        assertMapping(3, "27", "canViewMetadata", "canViewMetadata");
    }

    @Test
    public void setPermissionExistingEntry() throws Exception {
        FileUtils.write(project.getPermissionsPath().toFile(),
                "25,canViewOriginals,canViewOriginals", StandardCharsets.UTF_8, true);

        testHelper.indexExportData("mini_gilmer");
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "permissions", "set",
                "-id", "25",
                "-e", "canViewMetadata",
                "-a", "canViewMetadata"};
        executeExpectSuccess(args);
        assertMapping(0, "25", "canViewMetadata", "canViewMetadata");
    }

    @Test
    public void setPermissionNewEntry() throws Exception {
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "permissions", "generate",
                "-wd",
                "--everyone", "canViewOriginals",
                "--authenticated", "canViewOriginals"};
        executeExpectSuccess(args);

        testHelper.indexExportData("mini_gilmer");
        String[] args2 = new String[] {
                "-w", project.getProjectPath().toString(),
                "permissions", "set",
                "-id", "26",
                "-e", "canViewMetadata",
                "-a", "canViewMetadata"};
        executeExpectSuccess(args2);
        assertMapping(0, "default", "canViewOriginals", "canViewOriginals");
        assertMapping(1, "26", "canViewMetadata", "canViewMetadata");
    }

    @Test
    public void setPermissionNewGroupedWorkEntry() throws Exception {
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "permissions", "generate",
                "-wd",
                "--everyone", "canViewOriginals",
                "--authenticated", "canViewOriginals"};
        executeExpectSuccess(args);

        testHelper.indexExportData("grouped_gilmer");
        setupGroupedIndex();
        String[] args2 = new String[] {
                "-w", project.getProjectPath().toString(),
                "permissions", "set",
                "-id", "grp:groupa:group1",
                "-e", "canViewMetadata",
                "-a", "canViewMetadata"};
        executeExpectSuccess(args2);
        assertMapping(0, "default", "canViewOriginals", "canViewOriginals");
        assertMapping(1, "grp:groupa:group1", "canViewMetadata", "canViewMetadata");
    }

    @Test
    public void validateValidDefaultPermissions() throws Exception {
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "permissions", "generate",
                "-wd",
                "--everyone", "canViewMetadata",
                "--authenticated", "canViewMetadata"};
        executeExpectSuccess(args);

        String[] args2 = new String[] {
                "-w", project.getProjectPath().toString(),
                "permissions", "validate" };
        executeExpectSuccess(args2);

        assertOutputContains("PASS: Permissions mapping at path " + project.getPermissionsPath() + " is valid");
    }

    @Test
    public void validateInvalidDefaultPermissions() throws Exception {
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "permissions", "generate",
                "-wd",
                "--everyone", "canViewMetadata",
                "--authenticated", "canViewMetadata"};
        executeExpectSuccess(args);

        // Add a duplicate default permissions mapping
        FileUtils.write(project.getPermissionsPath().toFile(),
                "default,none,none", StandardCharsets.UTF_8, true);

        String[] args2 = new String[] {
                "-w", project.getProjectPath().toString(),
                "permissions", "validate" };
        executeExpectFailure(args2);

        assertOutputContains("FAIL: Permissions mapping at path " + project.getPermissionsPath()
                + " is invalid");
        assertOutputContains("Can only map default permissions once, encountered reassignment at line 3");
        assertEquals(2, output.split("    - ").length, "Must only be two errors: " + output);
    }

    @Test
    public void validateValidSetPermissions() throws Exception {
        FileUtils.write(project.getPermissionsPath().toFile(),
                "25,canViewOriginals,canViewOriginals", StandardCharsets.UTF_8, true);

        testHelper.indexExportData("mini_gilmer");
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "permissions", "set",
                "-id", "25",
                "--everyone", "canViewMetadata",
                "--authenticated", "canViewMetadata"};
        executeExpectSuccess(args);

        String[] args2 = new String[] {
                "-w", project.getProjectPath().toString(),
                "permissions", "validate" };
        executeExpectSuccess(args2);

        assertOutputContains("PASS: Permissions mapping at path " + project.getPermissionsPath() + " is valid");
    }

    private void assertMapping(int index, String id, String expectedEveryone, String expectedAuthenticated)
            throws IOException {
        var mappings = getMappings();
        PermissionsInfo.PermissionMapping mapping = mappings.get(index);
        assertEquals(id, mapping.getId());
        assertEquals(expectedEveryone, mapping.getEveryone());
        assertEquals(expectedAuthenticated, mapping.getAuthenticated());
    }

    private List<PermissionsInfo.PermissionMapping> getMappings() throws IOException {
        PermissionsInfo info = PermissionsService.loadMappings(project);
        return info.getMappings();
    }

    private void setupGroupedIndex() throws Exception {
        var options = new GroupMappingOptions();
        options.setGroupField("groupa");
        testHelper.getGroupMappingService().generateMapping(options);
        var syncOptions = new GroupMappingSyncOptions();
        syncOptions.setSortField("file");
        testHelper.getGroupMappingService().syncMappings(syncOptions);
    }
}
