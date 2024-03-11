package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.auth.api.UserRole;
import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.exceptions.StateAlreadyExistsException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.PermissionsInfo;
import edu.unc.lib.boxc.migration.cdm.options.GroupMappingOptions;
import edu.unc.lib.boxc.migration.cdm.options.GroupMappingSyncOptions;
import edu.unc.lib.boxc.migration.cdm.options.PermissionMappingOptions;
import edu.unc.lib.boxc.migration.cdm.test.BxcEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.SipServiceHelper;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static edu.unc.lib.boxc.auth.api.AccessPrincipalConstants.AUTHENTICATED_PRINC;
import static edu.unc.lib.boxc.auth.api.AccessPrincipalConstants.PUBLIC_PRINC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.MockitoAnnotations.openMocks;

public class PermissionsServiceTest {
    private static final String PROJECT_NAME = "proj";

    @TempDir
    public Path tmpFolder;

    private SipServiceHelper testHelper;
    private MigrationProject project;
    private PermissionsService service;
    private AutoCloseable closeable;

    @BeforeEach
    public void setup() throws Exception {
        closeable = openMocks(this);
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder, PROJECT_NAME, null, "user",
                CdmEnvironmentHelper.DEFAULT_ENV_ID, BxcEnvironmentHelper.DEFAULT_ENV_ID);
        testHelper = new SipServiceHelper(project, tmpFolder);
        service = new PermissionsService();
        service.setProject(project);
    }

    @AfterEach
    void closeService() throws Exception {
        closeable.close();
    }

    @Test
    public void generateNoDefaultPermissionsTest() throws Exception {
        Path permissionsMappingPath = project.getPermissionsPath();
        var options = new PermissionMappingOptions();

        service.generatePermissions(options);
        assertTrue(Files.exists(permissionsMappingPath));

        try (
                Reader reader = Files.newBufferedReader(permissionsMappingPath);
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT);
        ) {
            List<CSVRecord> rows = csvParser.getRecords();
            assertIterableEquals(Arrays.asList(PermissionsInfo.ID_FIELD, PermissionsInfo.OBJECT_TYPE,
                    PUBLIC_PRINC, AUTHENTICATED_PRINC), rows.get(0));
        }
    }

    @Test
    public void generateDefaultPermissionsTest() throws Exception {
        Path permissionsMappingPath = project.getPermissionsPath();
        var options = new PermissionMappingOptions();
        options.setWithDefault(true);
        options.setEveryone(UserRole.canViewMetadata);
        options.setAuthenticated(UserRole.canViewMetadata);

        service.generatePermissions(options);
        assertTrue(Files.exists(permissionsMappingPath));

        List<CSVRecord> rows = listCsvRecords(permissionsMappingPath);
        assertIterableEquals(Arrays.asList("default", "", "canViewMetadata", "canViewMetadata"), rows.get(0));
    }

    @Test
    public void generateDefaultPermissionsUnspecifiedTest() throws Exception {
        Path permissionsMappingPath = project.getPermissionsPath();
        var options = new PermissionMappingOptions();
        options.setWithDefault(true);

        service.generatePermissions(options);
        assertTrue(Files.exists(permissionsMappingPath));

        List<CSVRecord> rows = listCsvRecords(permissionsMappingPath);
        assertIterableEquals(Arrays.asList("default", "", "canViewOriginals", "canViewOriginals"), rows.get(0));
    }

    @Test
    public void generateDefaultPermissionsStaffOnlyTest() throws Exception {
        Path permissionsMappingPath = project.getPermissionsPath();
        var options = new PermissionMappingOptions();
        options.setWithDefault(true);
        options.setStaffOnly(true);

        service.generatePermissions(options);
        assertTrue(Files.exists(permissionsMappingPath));

        List<CSVRecord> rows = listCsvRecords(permissionsMappingPath);
        assertIterableEquals(Arrays.asList("default", "", "none", "none"), rows.get(0));
    }

    @Test
    public void generateDefaultPermissionsInvalidTest() throws Exception {
        var options = new PermissionMappingOptions();
        options.setWithDefault(true);
        options.setEveryone(UserRole.canViewMetadata);
        options.setAuthenticated(UserRole.canManage);

        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            service.generatePermissions(options);
        });

        String expectedMessage = "Assigned role value is invalid. Must be one of the following patron roles: " +
                "[none, canDiscover, canViewMetadata, canViewAccessCopies, canViewReducedQuality, canViewOriginals]";
        String actualMessage = exception.getMessage();
        assertEquals(expectedMessage, actualMessage);
    }

    @Test
    public void generateDefaultPermissionsWithoutForceFlagTest() throws Exception {
        writeCsv(mappingBody("default,,none,none"));

        var options = new PermissionMappingOptions();
        options.setWithDefault(true);
        options.setEveryone(UserRole.canViewMetadata);
        options.setAuthenticated(UserRole.canViewMetadata);

        Exception exception = assertThrows(StateAlreadyExistsException.class, () -> {
            service.generatePermissions(options);
        });

        String expectedMessage = "Cannot create permissions, a file already exists. Use the force flag to overwrite.";
        String actualMessage = exception.getMessage();
        assertEquals(expectedMessage, actualMessage);
    }

    @Test
    public void generateDefaultPermissionsWithForceFlagTest() throws Exception {
        Path permissionsMappingPath = project.getPermissionsPath();
        writeCsv(mappingBody("default,,none,none"));

        var options = new PermissionMappingOptions();
        options.setWithDefault(true);
        options.setEveryone(UserRole.canViewMetadata);
        options.setAuthenticated(UserRole.canViewMetadata);
        options.setForce(true);

        service.generatePermissions(options);
        assertTrue(Files.exists(permissionsMappingPath));

        List<CSVRecord> rows = listCsvRecords(permissionsMappingPath);
        assertIterableEquals(Arrays.asList("default", "", "canViewMetadata", "canViewMetadata"), rows.get(0));
    }

    @Test
    public void generateWorkPermissionsWithDefaultTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        Path permissionsMappingPath = project.getPermissionsPath();
        var options = new PermissionMappingOptions();
        options.setWithWorks(true);
        options.setWithDefault(true);
        options.setEveryone(UserRole.canViewMetadata);
        options.setAuthenticated(UserRole.canViewMetadata);

        service.generatePermissions(options);
        assertTrue(Files.exists(permissionsMappingPath));

        List<CSVRecord> rows = listCsvRecords(permissionsMappingPath);
        assertIterableEquals(Arrays.asList("default", "", "canViewMetadata", "canViewMetadata"), rows.get(0));
        assertIterableEquals(Arrays.asList("25", "work", "canViewMetadata", "canViewMetadata"), rows.get(1));
        assertIterableEquals(Arrays.asList("26", "work", "canViewMetadata", "canViewMetadata"), rows.get(2));
        assertIterableEquals(Arrays.asList("27", "work", "canViewMetadata", "canViewMetadata"), rows.get(3));
    }

    @Test
    public void generateWorkPermissionsGroupedWorksTest() throws Exception {
        testHelper.indexExportData("grouped_gilmer");
        setupGroupedIndex();
        Path permissionsMappingPath = project.getPermissionsPath();
        var options = new PermissionMappingOptions();
        options.setWithWorks(true);
        options.setEveryone(UserRole.canViewMetadata);
        options.setAuthenticated(UserRole.canViewMetadata);

        service.generatePermissions(options);
        assertTrue(Files.exists(permissionsMappingPath));

        List<CSVRecord> rows = listCsvRecords(permissionsMappingPath);
        assertIterableEquals(Arrays.asList("27", "work", "canViewMetadata", "canViewMetadata"), rows.get(0));
        assertIterableEquals(Arrays.asList("28", "work", "canViewMetadata", "canViewMetadata"), rows.get(1));
        assertIterableEquals(Arrays.asList("29", "work", "canViewMetadata", "canViewMetadata"), rows.get(2));
    }

    @Test
    public void generateFilePermissionsGroupedWorksTest() throws Exception {
        testHelper.indexExportData("grouped_gilmer");
        setupGroupedIndex();
        Path permissionsMappingPath = project.getPermissionsPath();
        var options = new PermissionMappingOptions();
        options.setWithFiles(true);
        options.setEveryone(UserRole.canViewMetadata);
        options.setAuthenticated(UserRole.canViewMetadata);

        service.generatePermissions(options);
        assertTrue(Files.exists(permissionsMappingPath));

        List<CSVRecord> rows = listCsvRecords(permissionsMappingPath);
        assertIterableEquals(Arrays.asList("25", "file", "canViewMetadata", "canViewMetadata"), rows.get(0));
        assertIterableEquals(Arrays.asList("26", "file", "canViewMetadata", "canViewMetadata"), rows.get(1));
    }

    @Test
    public void generateFilePermissionsCompoundObjectsTest() throws Exception {
        testHelper.indexExportData("mini_keepsakes");
        Path permissionsMappingPath = project.getPermissionsPath();
        var options = new PermissionMappingOptions();
        options.setWithFiles(true);
        options.setEveryone(UserRole.canViewMetadata);
        options.setAuthenticated(UserRole.canViewMetadata);

        service.generatePermissions(options);
        assertTrue(Files.exists(permissionsMappingPath));

        List<CSVRecord> rows = listCsvRecords(permissionsMappingPath);
        assertIterableEquals(Arrays.asList("602", "file", "canViewMetadata", "canViewMetadata"), rows.get(0));
        assertIterableEquals(Arrays.asList("603", "file", "canViewMetadata", "canViewMetadata"), rows.get(1));
        assertIterableEquals(Arrays.asList("605", "file", "canViewMetadata", "canViewMetadata"), rows.get(2));
        assertIterableEquals(Arrays.asList("606", "file", "canViewMetadata", "canViewMetadata"), rows.get(3));
    }

    @Test
    public void generateFilePermissionsWithDefaultTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        Path permissionsMappingPath = project.getPermissionsPath();
        var options = new PermissionMappingOptions();
        options.setWithDefault(true);
        options.setWithFiles(true);
        options.setEveryone(UserRole.canViewMetadata);
        options.setAuthenticated(UserRole.canViewMetadata);

        service.generatePermissions(options);
        assertTrue(Files.exists(permissionsMappingPath));

        List<CSVRecord> rows = listCsvRecords(permissionsMappingPath);
        assertIterableEquals(Arrays.asList("default", "", "canViewMetadata", "canViewMetadata"), rows.get(0));
    }

    @Test
    public void generateWorkAndFilePermissionsWithDefaultTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        Path permissionsMappingPath = project.getPermissionsPath();
        var options = new PermissionMappingOptions();
        options.setWithDefault(true);
        options.setWithFiles(true);
        options.setWithWorks(true);
        options.setEveryone(UserRole.canViewMetadata);
        options.setAuthenticated(UserRole.canViewMetadata);

        service.generatePermissions(options);
        assertTrue(Files.exists(permissionsMappingPath));

        List<CSVRecord> rows = listCsvRecords(permissionsMappingPath);
        assertIterableEquals(Arrays.asList("default", "", "canViewMetadata", "canViewMetadata"), rows.get(0));
        assertIterableEquals(Arrays.asList("25", "work", "canViewMetadata", "canViewMetadata"), rows.get(1));
        assertIterableEquals(Arrays.asList("26", "work", "canViewMetadata", "canViewMetadata"), rows.get(2));
        assertIterableEquals(Arrays.asList("27", "work", "canViewMetadata", "canViewMetadata"), rows.get(3));
    }

    @Test
    public void generateWorkAndFilePermissionsWithDefaultAndForceTest() throws Exception {
        Path permissionsMappingPath = project.getPermissionsPath();
        writeCsv(mappingBody("default,none,none,"));

        testHelper.indexExportData("mini_gilmer");
        var options = new PermissionMappingOptions();
        options.setWithDefault(true);
        options.setForce(true);
        options.setWithFiles(true);
        options.setWithWorks(true);
        options.setEveryone(UserRole.canViewMetadata);
        options.setAuthenticated(UserRole.canViewMetadata);

        service.generatePermissions(options);
        assertTrue(Files.exists(permissionsMappingPath));

        List<CSVRecord> rows = listCsvRecords(permissionsMappingPath);
        assertIterableEquals(Arrays.asList("default", "", "canViewMetadata", "canViewMetadata"), rows.get(0));
        assertIterableEquals(Arrays.asList("25", "work", "canViewMetadata", "canViewMetadata"), rows.get(1));
        assertIterableEquals(Arrays.asList("26", "work", "canViewMetadata", "canViewMetadata"), rows.get(2));
        assertIterableEquals(Arrays.asList("27", "work", "canViewMetadata", "canViewMetadata"), rows.get(3));
    }

    @Test
    public void generateWorkAndFilePermissionsCompoundObjectsTest() throws Exception {
        testHelper.indexExportData("mini_keepsakes");
        Path permissionsMappingPath = project.getPermissionsPath();
        var options = new PermissionMappingOptions();
        options.setWithWorks(true);
        options.setWithFiles(true);
        options.setEveryone(UserRole.canViewMetadata);
        options.setAuthenticated(UserRole.canViewMetadata);

        service.generatePermissions(options);
        assertTrue(Files.exists(permissionsMappingPath));

        List<CSVRecord> rows = listCsvRecords(permissionsMappingPath);
        assertIterableEquals(Arrays.asList("216", "work", "canViewMetadata", "canViewMetadata"), rows.get(0));
        assertIterableEquals(Arrays.asList("602", "file", "canViewMetadata", "canViewMetadata"), rows.get(1));
        assertIterableEquals(Arrays.asList("603", "file", "canViewMetadata", "canViewMetadata"), rows.get(2));
        assertIterableEquals(Arrays.asList("604", "work", "canViewMetadata", "canViewMetadata"), rows.get(3));
        assertIterableEquals(Arrays.asList("605", "file", "canViewMetadata", "canViewMetadata"), rows.get(4));
        assertIterableEquals(Arrays.asList("606", "file", "canViewMetadata", "canViewMetadata"), rows.get(5));
        assertIterableEquals(Arrays.asList("607", "work", "canViewMetadata", "canViewMetadata"), rows.get(6));
    }

    @Test
    public void loadPermissionMappingsTest() throws Exception {
        writeCsv(mappingBody("default,,canViewMetadata,canViewMetadata", "testId,work,none,none"));

        PermissionsInfo info = service.loadMappings(project);
        assertMappingPresent(info, "default", "canViewMetadata", "canViewMetadata");
        assertMappingPresent(info, "testId", "none", "none");

        PermissionsInfo.PermissionMapping mapping = info.getDefaultMapping();
        assertEquals("canViewMetadata", mapping.getEveryone());
        assertEquals("canViewMetadata", mapping.getAuthenticated());

        PermissionsInfo.PermissionMapping defaultMapping = info.getMappingByCdmId("default");
        assertEquals("canViewMetadata", defaultMapping.getEveryone());
        assertEquals("canViewMetadata", defaultMapping.getAuthenticated());
        PermissionsInfo.PermissionMapping testMapping = info.getMappingByCdmId("testId");
        assertEquals("none", testMapping.getEveryone());
        assertEquals("none", testMapping.getAuthenticated());
    }

    @Test
    public void setPermissionExistingEntryTest() throws Exception {
        writeCsv(mappingBody("default,,canViewMetadata,canViewMetadata", "25,work,none,none", "26,work,none,none",
                "27,work,none,none"));
        testHelper.indexExportData("mini_gilmer");
        Path permissionsMappingPath = project.getPermissionsPath();
        var options = new PermissionMappingOptions();
        options.setCdmId("25");
        options.setEveryone(UserRole.canViewMetadata);
        options.setAuthenticated(UserRole.canViewMetadata);

        service.setPermissions(options);
        assertTrue(Files.exists(permissionsMappingPath));

        List<CSVRecord> rows = listCsvRecords(permissionsMappingPath);
        assertIterableEquals(Arrays.asList("default", "", "canViewMetadata", "canViewMetadata"), rows.get(0));
        assertIterableEquals(Arrays.asList("25", "work", "canViewMetadata", "canViewMetadata"), rows.get(1));
        assertIterableEquals(Arrays.asList("26", "work", "none", "none"), rows.get(2));
        assertIterableEquals(Arrays.asList("27", "work", "none", "none"), rows.get(3));
    }

    @Test
    public void setPermissionsGroupedWorkEntryTest() throws Exception {
        writeCsv(mappingBody("default,,canViewMetadata,canViewMetadata", "26,work,none,none", "27,work,none,none"));
        testHelper.indexExportData("grouped_gilmer");
        setupGroupedIndex();
        Path permissionsMappingPath = project.getPermissionsPath();
        var options = new PermissionMappingOptions();
        options.setCdmId("grp:groupa:group1");
        options.setEveryone(UserRole.canViewMetadata);
        options.setAuthenticated(UserRole.canViewMetadata);

        service.setPermissions(options);
        assertTrue(Files.exists(permissionsMappingPath));

        List<CSVRecord> rows = listCsvRecords(permissionsMappingPath);
        assertIterableEquals(Arrays.asList("default", "", "canViewMetadata", "canViewMetadata"), rows.get(0));
        assertIterableEquals(Arrays.asList("26", "work", "none", "none"), rows.get(1));
        assertIterableEquals(Arrays.asList("27", "work", "none", "none"), rows.get(2));
        assertIterableEquals(Arrays.asList("grp:groupa:group1", "work", "canViewMetadata", "canViewMetadata"), rows.get(3));
    }

    @Test
    public void setPermissionNewEntryTest() throws Exception {
        writeCsv(mappingBody("default,,canViewMetadata,canViewMetadata", "25,work,none,none", "26,work,none,none"));
        testHelper.indexExportData("mini_gilmer");
        Path permissionsMappingPath = project.getPermissionsPath();
        var options = new PermissionMappingOptions();
        options.setCdmId("27");
        options.setEveryone(UserRole.canViewMetadata);
        options.setAuthenticated(UserRole.canViewMetadata);

        service.setPermissions(options);
        assertTrue(Files.exists(permissionsMappingPath));

        List<CSVRecord> rows = listCsvRecords(permissionsMappingPath);
        assertIterableEquals(Arrays.asList("default", "", "canViewMetadata", "canViewMetadata"), rows.get(0));
        assertIterableEquals(Arrays.asList("25", "work", "none", "none"), rows.get(1));
        assertIterableEquals(Arrays.asList("26", "work", "none", "none"), rows.get(2));
        assertIterableEquals(Arrays.asList("27", "work", "canViewMetadata", "canViewMetadata"), rows.get(3));
    }

    @Test
    public void setPermissionInvalidIdTest() throws Exception {
        writeCsv(mappingBody("default,,canViewMetadata,canViewMetadata", "25,work,none,none", "26,work,none,none"));
        testHelper.indexExportData("mini_gilmer");
        var options = new PermissionMappingOptions();
        options.setCdmId("28");
        options.setEveryone(UserRole.canViewMetadata);
        options.setAuthenticated(UserRole.canViewMetadata);

        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            service.setPermissions(options);
        });

        String expectedMessage = "Id 28 does not exist in this project.";
        String actualMessage = exception.getMessage();
        assertEquals(expectedMessage, actualMessage);
    }

    @Test
    public void setPermissionNoCsvTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        var options = new PermissionMappingOptions();
        options.setCdmId("27");
        options.setEveryone(UserRole.canViewMetadata);
        options.setAuthenticated(UserRole.canViewMetadata);

        Exception exception = assertThrows(InvalidProjectStateException.class, () -> {
            service.setPermissions(options);
        });

        String expectedMessage = "Permissions csv does not exist.";
        String actualMessage = exception.getMessage();
        assertEquals(expectedMessage, actualMessage);
    }

    @Test
    public void setPermissionWithWorksDefault() throws Exception {
        writeCsv(mappingBody("default,,canViewMetadata,canViewMetadata", "26,work,none,none"));
        testHelper.indexExportData("mini_gilmer");
        Path permissionsMappingPath = project.getPermissionsPath();
        var options = new PermissionMappingOptions();
        options.setWithWorks(true);
        options.setEveryone(UserRole.canViewMetadata);
        options.setAuthenticated(UserRole.canViewMetadata);

        service.setPermissions(options);
        assertTrue(Files.exists(permissionsMappingPath));

        List<CSVRecord> rows = listCsvRecords(permissionsMappingPath);
        assertIterableEquals(Arrays.asList("default", "", "canViewMetadata", "canViewMetadata"), rows.get(0));
        assertIterableEquals(Arrays.asList("25", "work", "canViewMetadata", "canViewMetadata"), rows.get(1));
        assertIterableEquals(Arrays.asList("26", "work", "canViewMetadata", "canViewMetadata"), rows.get(2));
        assertIterableEquals(Arrays.asList("27", "work", "canViewMetadata", "canViewMetadata"), rows.get(3));
    }

    @Test
    public void setPermissionsWithFilesGroupedWork() throws Exception {
        writeCsv(mappingBody("default,,canViewMetadata,canViewMetadata"));
        testHelper.indexExportData("grouped_gilmer");
        setupGroupedIndex();
        Path permissionsMappingPath = project.getPermissionsPath();
        var options = new PermissionMappingOptions();
        options.setWithFiles(true);
        options.setEveryone(UserRole.canViewMetadata);
        options.setAuthenticated(UserRole.canViewMetadata);

        service.setPermissions(options);
        assertTrue(Files.exists(permissionsMappingPath));

        List<CSVRecord> rows = listCsvRecords(permissionsMappingPath);
        assertIterableEquals(Arrays.asList("default", "", "canViewMetadata", "canViewMetadata"), rows.get(0));
        assertIterableEquals(Arrays.asList("25", "file", "canViewMetadata", "canViewMetadata"), rows.get(1));
        assertIterableEquals(Arrays.asList("26", "file", "canViewMetadata", "canViewMetadata"), rows.get(2));
    }

    @Test
    public void setPermissionsWithFilesCompoundObjects() throws Exception {
        writeCsv(mappingBody("603,file,canViewMetadata,canViewMetadata"));
        testHelper.indexExportData("mini_keepsakes");
        Path permissionsMappingPath = project.getPermissionsPath();
        var options = new PermissionMappingOptions();
        options.setWithFiles(true);
        options.setEveryone(UserRole.canViewMetadata);
        options.setAuthenticated(UserRole.canViewMetadata);

        service.setPermissions(options);
        assertTrue(Files.exists(permissionsMappingPath));

        List<CSVRecord> rows = listCsvRecords(permissionsMappingPath);
        assertIterableEquals(Arrays.asList("602", "file", "canViewMetadata", "canViewMetadata"), rows.get(0));
        assertIterableEquals(Arrays.asList("603", "file", "canViewMetadata", "canViewMetadata"), rows.get(1));
        assertIterableEquals(Arrays.asList("605", "file", "canViewMetadata", "canViewMetadata"), rows.get(2));
        assertIterableEquals(Arrays.asList("606", "file", "canViewMetadata", "canViewMetadata"), rows.get(3));
    }

    @Test
    public void setPermissionsWithWorksAndFiles() throws Exception {
        writeCsv(mappingBody("604,work,canViewMetadata,canViewMetadata"));
        testHelper.indexExportData("mini_keepsakes");
        Path permissionsMappingPath = project.getPermissionsPath();
        var options = new PermissionMappingOptions();
        options.setWithWorks(true);
        options.setWithFiles(true);
        options.setEveryone(UserRole.canViewMetadata);
        options.setAuthenticated(UserRole.canViewMetadata);

        service.setPermissions(options);
        assertTrue(Files.exists(permissionsMappingPath));

        List<CSVRecord> rows = listCsvRecords(permissionsMappingPath);
        assertIterableEquals(Arrays.asList("216", "work", "canViewMetadata", "canViewMetadata"), rows.get(0));
        assertIterableEquals(Arrays.asList("602", "file", "canViewMetadata", "canViewMetadata"), rows.get(1));
        assertIterableEquals(Arrays.asList("603", "file", "canViewMetadata", "canViewMetadata"), rows.get(2));
        assertIterableEquals(Arrays.asList("604", "work", "canViewMetadata", "canViewMetadata"), rows.get(3));
        assertIterableEquals(Arrays.asList("605", "file", "canViewMetadata", "canViewMetadata"), rows.get(4));
        assertIterableEquals(Arrays.asList("606", "file", "canViewMetadata", "canViewMetadata"), rows.get(5));
        assertIterableEquals(Arrays.asList("607", "work", "canViewMetadata", "canViewMetadata"), rows.get(6));
    }

    @Test
    public void setPermissionsExistingWorksNewFiles() throws Exception {
        writeCsv(mappingBody("216,work,canViewMetadata,canViewMetadata",
                "604,work,canViewMetadata,canViewMetadata", "607,work,canViewMetadata,canViewMetadata"));
        testHelper.indexExportData("mini_keepsakes");
        Path permissionsMappingPath = project.getPermissionsPath();
        var options = new PermissionMappingOptions();
        options.setWithFiles(true);
        options.setEveryone(UserRole.canViewMetadata);
        options.setAuthenticated(UserRole.canViewMetadata);

        service.setPermissions(options);
        assertTrue(Files.exists(permissionsMappingPath));

        List<CSVRecord> rows = listCsvRecords(permissionsMappingPath);
        assertIterableEquals(Arrays.asList("216", "work", "canViewMetadata", "canViewMetadata"), rows.get(0));
        assertIterableEquals(Arrays.asList("602", "file", "canViewMetadata", "canViewMetadata"), rows.get(1));
        assertIterableEquals(Arrays.asList("603", "file", "canViewMetadata", "canViewMetadata"), rows.get(2));
        assertIterableEquals(Arrays.asList("604", "work", "canViewMetadata", "canViewMetadata"), rows.get(3));
        assertIterableEquals(Arrays.asList("605", "file", "canViewMetadata", "canViewMetadata"), rows.get(4));
        assertIterableEquals(Arrays.asList("606", "file", "canViewMetadata", "canViewMetadata"), rows.get(5));
        assertIterableEquals(Arrays.asList("607", "work", "canViewMetadata", "canViewMetadata"), rows.get(6));
    }

    private String mappingBody(String... rows) {
        return String.join(",", PermissionsInfo.CSV_HEADERS) + "\n"
                + String.join("\n", rows);
    }

    private void writeCsv(String mappingBody) throws IOException {
        FileUtils.write(project.getPermissionsPath().toFile(),
                mappingBody, StandardCharsets.UTF_8);
        ProjectPropertiesSerialization.write(project);
    }

    private List<CSVRecord> listCsvRecords(Path permissionsMappingPath) throws Exception {
        List<CSVRecord> rows;
        try (
                Reader reader = Files.newBufferedReader(permissionsMappingPath);
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                        .withFirstRecordAsHeader()
                        .withHeader(PermissionsInfo.CSV_HEADERS)
                        .withTrim());
        ) {
            rows = csvParser.getRecords();
        }
        return rows;
    }

    private void assertMappingPresent(PermissionsInfo info, String cdmid, String everyoneValue, String authenticatedValue) {
        List<PermissionsInfo.PermissionMapping> mappings = info.getMappings();
        PermissionsInfo.PermissionMapping mapping = mappings.stream().filter(m -> m.getId().equals(cdmid)).findFirst().get();

        assertEquals(everyoneValue, mapping.getEveryone());
        assertEquals(authenticatedValue, mapping.getAuthenticated());
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
