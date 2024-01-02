package edu.unc.lib.boxc.migration.cdm;

import edu.unc.lib.boxc.migration.cdm.model.PermissionsInfo;
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
    public void generateDefaultPermissions() throws Exception {
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "permissions", "generate",
                "-id", "default",
                "--everyone", "canViewMetadata",
                "--authenticated", "canViewMetadata"};
        executeExpectSuccess(args);
        assertDefaultMapping("default", "canViewMetadata",
                "canViewMetadata");
    }

    @Test
    public void generateDefaultPermissionsUnspecified() throws Exception {
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "permissions", "generate",
                "-id", "default"};
        executeExpectSuccess(args);
        assertDefaultMapping("default", "canViewOriginals",
                "canViewOriginals");
    }

    @Test
    public void generateDefaultPermissionsStaffOnly() throws Exception {
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "permissions", "generate",
                "-id", "default",
                "-so"};
        executeExpectSuccess(args);
        assertDefaultMapping("default", "none", "none");
    }

    @Test
    public void generateDefaultPermissionsInvalid() throws Exception {
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "permissions", "generate",
                "-id", "default",
                "--everyone", "canViewMetadata",
                "--authenticated", "canManage"};
        executeExpectFailure(args);
        assertOutputContains("Assigned role value is invalid. Must be one of the following patron roles: " +
                "[none, canDiscover, canViewMetadata, canViewAccessCopies, canViewOriginals]");
    }

    @Test
    public void generateDefaultPermissionsWithoutForceFlag() throws Exception {
        FileUtils.write(project.getPermissionsPath().toFile(),
                "default,canViewMetadata,canViewMetadata", StandardCharsets.UTF_8, true);

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "permissions", "generate",
                "-id", "default",
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
                "-id", "default",
                "--everyone", "canViewOriginals",
                "--authenticated", "canViewOriginals",
                "--force"};
        executeExpectSuccess(args);
        assertDefaultMapping("default", "canViewOriginals",
                "canViewOriginals");
    }

    private void assertDefaultMapping(String defaultValue, String expectedEveryone, String expectedAuthenticated)
            throws IOException {
        var mappings = getMappings();
        assertMappingCount(mappings, 1);
        PermissionsInfo.PermissionMapping mapping = mappings.get(0);
        assertEquals(PermissionsInfo.DEFAULT_ID, mapping.getId());
        assertEquals(expectedEveryone, mapping.getEveryone());
        assertEquals(expectedAuthenticated, mapping.getAuthenticated());
    }

    private List<PermissionsInfo.PermissionMapping> getMappings() throws IOException {
        PermissionsInfo info = PermissionsService.loadMappings(project);
        return info.getMappings();
    }

    private void assertMappingCount(List<PermissionsInfo.PermissionMapping> mappings, int count) {
        assertEquals(count, mappings.size());
    }
}
