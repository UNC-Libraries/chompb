package edu.unc.lib.boxc.migration.cdm.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import edu.unc.lib.boxc.migration.cdm.options.AddSourceFileMappingOptions;
import edu.unc.lib.boxc.migration.cdm.options.GenerateSourceFileMappingOptions;
import edu.unc.lib.boxc.migration.cdm.test.BxcEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.SipServiceHelper;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProjectProperties;
import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo;
import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo.SourceFileMapping;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;

/**
 * @author bbpennel
 */
public class SourceFileServiceTest {
    private static final String PROJECT_NAME = "proj";

    @TempDir
    public Path tmpFolder;

    private MigrationProject project;
    private CdmIndexService indexService;
    private CdmFieldService fieldService;
    private SourceFileService service;
    private SipServiceHelper testHelper;

    private Path basePath;

    @BeforeEach
    public void setup() throws Exception {
        project = MigrationProjectFactory.createCdmMigrationProject(
                tmpFolder, PROJECT_NAME, null, "user",
                CdmEnvironmentHelper.DEFAULT_ENV_ID, BxcEnvironmentHelper.DEFAULT_ENV_ID);
        Files.createDirectories(project.getExportPath());

        basePath = tmpFolder.resolve("testFolder");
        Files.createDirectory(basePath);
        testHelper = new SipServiceHelper(project, basePath);

        service = testHelper.getSourceFileService();
    }

    @Test
    public void generateNoIndexTest() throws Exception {
        GenerateSourceFileMappingOptions options = makeDefaultOptions();

        try {
            service.generateMapping(options);
            fail();
        } catch (InvalidProjectStateException e) {
            assertExceptionContains("Project must be indexed", e);
            assertMappedDateNotPresent();
        }
    }

    @Test
    public void generateBasePathIsNotADirectoryTest() throws Exception {
        setIndexedDate();
        GenerateSourceFileMappingOptions options = makeDefaultOptions();
        Files.delete(basePath);
        Files.createFile(basePath);

        try {
            service.generateMapping(options);
            fail();
        } catch (IllegalArgumentException e) {
            assertExceptionContains("Base path must be a directory", e);
            assertMappedDateNotPresent();
        }
    }

    @Test
    public void generateBasePathDoesNotExistTest() throws Exception {
        setIndexedDate();
        GenerateSourceFileMappingOptions options = makeDefaultOptions();
        Files.delete(basePath);

        try {
            service.generateMapping(options);
            fail();
        } catch (IllegalArgumentException e) {
            assertExceptionContains("Base path must be a directory", e);
            assertMappedDateNotPresent();
        }
    }

    @Test
    public void generateNoSourceFilesTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        GenerateSourceFileMappingOptions options = makeDefaultOptions();
        service.generateMapping(options);

        SourceFilesInfo info = service.loadMappings();
        assertMappingPresent(info, "25", "276_182_E.tif", null);
        assertMappingPresent(info, "26", "276_183_E.tif", null);
        assertMappingPresent(info, "27", "276_203_E.tif", null);

        assertMappedDatePresent();
    }

    @Test
    public void generateExactMatchTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        GenerateSourceFileMappingOptions options = makeDefaultOptions();
        Path srcPath1 = testHelper.addSourceFile("276_182_E.tif");

        service.generateMapping(options);

        SourceFilesInfo info = service.loadMappings();
        assertMappingPresent(info, "25", "276_182_E.tif", srcPath1);
        assertMappingPresent(info, "26", "276_183_E.tif", null);
        assertMappingPresent(info, "27", "276_203_E.tif", null);

        assertMappedDatePresent();
    }

    @Test
    public void generateTransformedMatchTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        GenerateSourceFileMappingOptions options = makeDefaultOptions();
        options.setFieldMatchingPattern("(\\d+)\\_([^_]+)\\_E.tif");
        options.setFilenameTemplate("00$1_op0$2_0001_e.tif");

        Path srcPath1 = testHelper.addSourceFile("00276_op0182_0001_e.tif");
        Path srcPath3 = testHelper.addSourceFile("00276_op0203_0001_e.tif");

        service.generateMapping(options);

        SourceFilesInfo info = service.loadMappings();
        assertMappingPresent(info, "25", "276_182_E.tif", srcPath1);
        assertMappingPresent(info, "26", "276_183_E.tif", null);
        assertMappingPresent(info, "27", "276_203_E.tif", srcPath3);

        assertMappedDatePresent();
    }

    @Test
    public void generateNestedMatchesTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        GenerateSourceFileMappingOptions options = makeDefaultOptions();
        options.setPathPattern("**/*.tif");
        Path srcPath1 = testHelper.addSourceFile("nested/path/276_182_E.tif");
        Path srcPath2 = testHelper.addSourceFile("276_183_E.tif");

        service.generateMapping(options);

        SourceFilesInfo info = service.loadMappings();
        assertMappingPresent(info, "25", "276_182_E.tif", srcPath1);
        assertMappingPresent(info, "26", "276_183_E.tif", srcPath2);
        assertMappingPresent(info, "27", "276_203_E.tif", null);

        assertMappedDatePresent();
    }

    @Test
    public void generateTransformedNestedMatchTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        GenerateSourceFileMappingOptions options = makeDefaultOptions();
        options.setFieldMatchingPattern("(\\d+)\\_([^_]+)\\_E.tif");
        options.setFilenameTemplate("00$1_op0$2_0001_e.tif");
        Path srcPath1 = testHelper.addSourceFile("nested/path/00276_op0182_0001_e.tif");
        // Add extra file that does not match the pattern
        testHelper.addSourceFile("nested/path/276_182_E.tif");

        service.generateMapping(options);

        SourceFilesInfo info = service.loadMappings();
        assertMappingPresent(info, "25", "276_182_E.tif", srcPath1);
        assertMappingPresent(info, "26", "276_183_E.tif", null);
        assertMappingPresent(info, "27", "276_203_E.tif", null);

        assertMappedDatePresent();
    }

    @Test
    public void generateMultipleMatchesForSameObjectTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        GenerateSourceFileMappingOptions options = makeDefaultOptions();
        options.setPathPattern("**/*.tif");
        Path srcPath1 = testHelper.addSourceFile("nested/path/276_182_E.tif");
        Path srcPath1Dupe = testHelper.addSourceFile("nested/276_182_E.tif");
        Path srcPath2 = testHelper.addSourceFile("276_183_E.tif");

        service.generateMapping(options);

        SourceFilesInfo info = service.loadMappings();
        assertMappingPresent(info, "25", "276_182_E.tif", null, srcPath1, srcPath1Dupe);
        assertMappingPresent(info, "26", "276_183_E.tif", srcPath2);
        assertMappingPresent(info, "27", "276_203_E.tif", null);

        assertMappedDatePresent();
    }

    @Test
    public void generateDryRunSummaryTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        GenerateSourceFileMappingOptions options = makeDefaultOptions();
        options.setDryRun(true);
        testHelper.addSourceFile("276_182_E.tif");

        service.generateMapping(options);

        assertFalse(Files.exists(project.getSourceFilesMappingPath()));
        assertMappedDateNotPresent();
    }

    @Test
    public void generateMappingExistsTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        GenerateSourceFileMappingOptions options = makeDefaultOptions();
        Path srcPath1 = testHelper.addSourceFile("276_182_E.tif");

        service.generateMapping(options);

        SourceFilesInfo info = service.loadMappings();
        assertMappingPresent(info, "25", "276_182_E.tif", srcPath1);
        assertMappingPresent(info, "26", "276_183_E.tif", null);
        assertMappingPresent(info, "27", "276_203_E.tif", null);

        // Add extra matching source file, which should not show up during rejected run
        Path srcPath2 = testHelper.addSourceFile("276_183_E.tif");
        try {
            service.generateMapping(options);
            fail();
        } catch (MigrationException e) {
            assertTrue(e.getMessage().contains("Cannot create mapping, a file already exists"));
        }

        SourceFilesInfo info2 = service.loadMappings();
        assertMappingPresent(info2, "25", "276_182_E.tif", srcPath1);
        assertMappingPresent(info2, "26", "276_183_E.tif", null);
        assertMappingPresent(info2, "27", "276_203_E.tif", null);

        // Try again with force
        options.setForce(true);
        service.generateMapping(options);

        SourceFilesInfo info3 = service.loadMappings();
        assertMappingPresent(info3, "25", "276_182_E.tif", srcPath1);
        assertMappingPresent(info3, "26", "276_183_E.tif", srcPath2);
        assertMappingPresent(info3, "27", "276_203_E.tif", null);

        assertMappedDatePresent();
    }

    @Test
    public void generateMatchesLowercaseTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        GenerateSourceFileMappingOptions options = makeDefaultOptions();
        options.setLowercaseTemplate(true);
        Path srcPath1 = testHelper.addSourceFile("276_182_e.tif");
        Path srcPath2 = testHelper.addSourceFile("276_183_E.tif");

        service.generateMapping(options);

        SourceFilesInfo info = service.loadMappings();
        assertMappingPresent(info, "25", "276_182_E.tif", srcPath1);
        assertMappingPresent(info, "26", "276_183_E.tif", srcPath2);
        assertMappingPresent(info, "27", "276_203_E.tif", null);

        assertMappedDatePresent();
    }

    @Test
    public void generateUpdateNoExistingFileTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        GenerateSourceFileMappingOptions options = makeDefaultOptions();
        options.setUpdate(true);
        Path srcPath1 = testHelper.addSourceFile("276_182_E.tif");

        service.generateMapping(options);

        SourceFilesInfo info = service.loadMappings();
        assertMappingPresent(info, "25", "276_182_E.tif", srcPath1);
        assertMappingPresent(info, "26", "276_183_E.tif", null);
        assertMappingPresent(info, "27", "276_203_E.tif", null);

        assertMappedDatePresent();
    }

    @Test
    public void generateUpdateExistingNoChangesTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        GenerateSourceFileMappingOptions options = makeDefaultOptions();
        Path srcPath1 = testHelper.addSourceFile("276_182_E.tif");

        service.generateMapping(options);

        SourceFilesInfo info = service.loadMappings();
        assertMappingPresent(info, "25", "276_182_E.tif", srcPath1);
        assertMappingPresent(info, "26", "276_183_E.tif", null);
        assertMappingPresent(info, "27", "276_203_E.tif", null);

        assertMappedDatePresent();

        options.setUpdate(true);
        service.generateMapping(options);

        SourceFilesInfo info2 = service.loadMappings();
        assertMappingPresent(info2, "25", "276_182_E.tif", srcPath1);
        assertMappingPresent(info2, "26", "276_183_E.tif", null);
        assertMappingPresent(info2, "27", "276_203_E.tif", null);
    }

    @Test
    public void generateUpdateAddNewSourceFileTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        GenerateSourceFileMappingOptions options = makeDefaultOptions();
        Path srcPath1 = testHelper.addSourceFile("276_182_E.tif");

        service.generateMapping(options);

        Path srcPath2 = testHelper.addSourceFile("276_183_E.tif");
        options.setUpdate(true);
        service.generateMapping(options);

        SourceFilesInfo info2 = service.loadMappings();
        assertMappingPresent(info2, "25", "276_182_E.tif", srcPath1);
        assertMappingPresent(info2, "26", "276_183_E.tif", srcPath2);
        assertMappingPresent(info2, "27", "276_203_E.tif", null);
    }

    @Test
    public void generateUpdateChangeSourceFileTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        GenerateSourceFileMappingOptions options = makeDefaultOptions();
        options.setPathPattern("**/*");
        Path srcPath1 = testHelper.addSourceFile("276_182_E.tif");

        service.generateMapping(options);

        Files.delete(srcPath1);
        testHelper.addSourceFile("nested/276_182_E.tif");
        options.setUpdate(true);
        service.generateMapping(options);

        SourceFilesInfo info2 = service.loadMappings();
        // Should still list the original source path
        assertMappingPresent(info2, "25", "276_182_E.tif", srcPath1);
        assertMappingPresent(info2, "26", "276_183_E.tif", null);
        assertMappingPresent(info2, "27", "276_203_E.tif", null);
    }

    @Test
    public void generateUpdateChangeSourceFileWithForceTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        GenerateSourceFileMappingOptions options = makeDefaultOptions();
        options.setPathPattern("**/*");
        Path srcPath1 = testHelper.addSourceFile("276_182_E.tif");

        service.generateMapping(options);

        Files.delete(srcPath1);
        Path srcPath2 = testHelper.addSourceFile("nested/276_182_E.tif");
        options.setUpdate(true);
        options.setForce(true);
        service.generateMapping(options);

        SourceFilesInfo info2 = service.loadMappings();
        // Should be using the new path
        assertMappingPresent(info2, "25", "276_182_E.tif", srcPath2);
        assertMappingPresent(info2, "26", "276_183_E.tif", null);
        assertMappingPresent(info2, "27", "276_203_E.tif", null);
    }

    @Test
    public void generateUpdateAddPotentialMatchToExistingMatchTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        GenerateSourceFileMappingOptions options = makeDefaultOptions();
        options.setPathPattern("**/*");
        Path srcPath1 = testHelper.addSourceFile("276_182_E.tif");

        service.generateMapping(options);

        testHelper.addSourceFile("nested/276_182_E.tif");
        options.setUpdate(true);
        service.generateMapping(options);

        SourceFilesInfo info2 = service.loadMappings();
        // Should be using the new path
        assertMappingPresent(info2, "25", "276_182_E.tif", srcPath1);
        assertMappingPresent(info2, "26", "276_183_E.tif", null);
        assertMappingPresent(info2, "27", "276_203_E.tif", null);
    }

    @Test
    public void generateUpdateAddPotentialMatchTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        GenerateSourceFileMappingOptions options = makeDefaultOptions();
        options.setPathPattern("**/*");

        service.generateMapping(options);
        SourceFilesInfo info = service.loadMappings();
        assertMappingPresent(info, "25", "276_182_E.tif", null);
        assertMappingPresent(info, "26", "276_183_E.tif", null);
        assertMappingPresent(info, "27", "276_203_E.tif", null);

        Path srcPath1 = testHelper.addSourceFile("276_182_E.tif");
        Path srcPath2 = testHelper.addSourceFile("nested/276_182_E.tif");
        options.setUpdate(true);
        service.generateMapping(options);

        SourceFilesInfo info2 = service.loadMappings();
        // Should be using the new path
        assertMappingPresent(info2, "25", "276_182_E.tif", null, srcPath1, srcPath2);
        assertMappingPresent(info2, "26", "276_183_E.tif", null);
        assertMappingPresent(info2, "27", "276_203_E.tif", null);
    }

    @Test
    public void generateUpdateAddPotentialMatchWithExistingPotentialTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        GenerateSourceFileMappingOptions options = makeDefaultOptions();
        options.setPathPattern("**/*");

        Path srcPath1 = testHelper.addSourceFile("276_182_E.tif");
        Path srcPath2 = testHelper.addSourceFile("nested/276_182_E.tif");

        service.generateMapping(options);
        SourceFilesInfo info = service.loadMappings();
        assertMappingPresent(info, "25", "276_182_E.tif", null, srcPath1, srcPath2);
        assertMappingPresent(info, "26", "276_183_E.tif", null);
        assertMappingPresent(info, "27", "276_203_E.tif", null);

        Path srcPath3 = testHelper.addSourceFile("another/276_182_E.tif");
        options.setUpdate(true);
        service.generateMapping(options);

        SourceFilesInfo info2 = service.loadMappings();
        // Should be using the new path
        assertMappingPresent(info2, "25", "276_182_E.tif", null, srcPath1, srcPath2, srcPath3);
        assertMappingPresent(info2, "26", "276_183_E.tif", null);
        assertMappingPresent(info2, "27", "276_203_E.tif", null);
    }

    @Test
    public void generateUpdateAddNewRecordsTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        GenerateSourceFileMappingOptions options = makeDefaultOptions();
        Path srcPath1 = testHelper.addSourceFile("276_182_E.tif");

        service.generateMapping(options);

        // Add more exported objects
        testHelper.indexExportData("gilmer");

        Path srcPath2 = testHelper.addSourceFile("276_245a_E.tif");
        options.setUpdate(true);
        service.generateMapping(options);

        SourceFilesInfo info2 = service.loadMappings();
        assertMappingPresent(info2, "25", "276_182_E.tif", srcPath1);
        assertMappingPresent(info2, "26", "276_183_E.tif", null);
        assertMappingPresent(info2, "27", "276_203_E.tif", null);
        assertMappingPresent(info2, "28", "276_241_E.tif", null);
        assertMappingPresent(info2, "29", "276_245a_E.tif", srcPath2);
    }

    @Test
    public void generateUpdateDryRunAddNewSourceFileTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        GenerateSourceFileMappingOptions options = makeDefaultOptions();
        Path srcPath1 = testHelper.addSourceFile("276_182_E.tif");

        service.generateMapping(options);

        testHelper.addSourceFile("276_183_E.tif");
        options.setUpdate(true);
        options.setDryRun(true);
        service.generateMapping(options);

        SourceFilesInfo info2 = service.loadMappings();
        assertMappingPresent(info2, "25", "276_182_E.tif", srcPath1);
        // Mapping should be unchanged
        assertMappingPresent(info2, "26", "276_183_E.tif", null);
        assertMappingPresent(info2, "27", "276_203_E.tif", null);
    }

    @Test
    public void generateCompoundExactMatchTest() throws Exception {
        testHelper.indexExportData(Paths.get("src/test/resources/keepsakes_fields.csv"), "mini_keepsakes");
        GenerateSourceFileMappingOptions options = makeDefaultOptions();
        options.setExportField("filena");
        Path srcPath1 = testHelper.addSourceFile("nccg_ck_09.tif");
        Path srcPath2 = testHelper.addSourceFile("nccg_ck_1042-22_v1.tif");
        Path srcPath3 = testHelper.addSourceFile("nccg_ck_1042-22_v2.tif");
        Path srcPath4 = testHelper.addSourceFile("nccg_ck_549-4_v1.tif");
        Path srcPath5 = testHelper.addSourceFile("nccg_ck_549-4_v2.tif");

        service.generateMapping(options);

        SourceFilesInfo info = service.loadMappings();
        assertMappingPresent(info, "216", "nccg_ck_09.tif", srcPath1);
        assertMappingPresent(info, "602", "nccg_ck_1042-22_v1.tif", srcPath2);
        assertMappingPresent(info, "603", "nccg_ck_1042-22_v2.tif", srcPath3);
        assertMappingPresent(info, "605", "nccg_ck_549-4_v1.tif", srcPath4);
        assertMappingPresent(info, "606", "nccg_ck_549-4_v2.tif", srcPath5);
        // There must not be mapping entries for the compound objects, only for the children
        assertEquals(5, info.getMappings().size());

        assertMappedDatePresent();
    }

    @Test
    public void generateBlankTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        GenerateSourceFileMappingOptions options = new GenerateSourceFileMappingOptions();
        options.setPopulateBlank(true);

        service.generateMapping(options);

        SourceFilesInfo info = service.loadMappings();
        assertMappingPresent(info, "25", "", null);
        assertMappingPresent(info, "26", "", null);
        assertMappingPresent(info, "27", "", null);

        assertMappedDatePresent();
    }

    @Test
    public void generateRespectsForceFlagTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        GenerateSourceFileMappingOptions options = new GenerateSourceFileMappingOptions();
        options.setPopulateBlank(true);

        service.generateMapping(options);

        SourceFilesInfo info = service.loadMappings();
        assertMappingPresent(info, "25", "", null);
        assertMappingPresent(info, "26", "", null);
        assertMappingPresent(info, "27", "", null);
        assertEquals(3, info.getMappings().size());

        try {
            service.generateMapping(options);
            fail();
        } catch (MigrationException e) {
            assertTrue(e.getMessage().contains("Cannot create mapping, a file already exists"));
        }

        options.setForce(true);
        service.generateMapping(options);

        SourceFilesInfo info2 = service.loadMappings();
        assertMappingPresent(info2, "25", "", null);
        assertMappingPresent(info2, "26", "", null);
        assertMappingPresent(info2, "27", "", null);
        assertEquals(3, info2.getMappings().size());
    }

    @Test
    public void addToMappingTest() throws Exception {
        AddSourceFileMappingOptions options = new AddSourceFileMappingOptions();
        options.setBasePath(Path.of("src/test/resources/files"));
        options.setExtensions(Collections.singletonList("tif"));
        options.setOptionalIdPrefix("test");

        service.addToMapping(options);

        List<Path> testSourcePaths = Arrays.asList(Path.of("D2_035_Varners_DrugStore_interior.tif"),
                Path.of("MJM_7_016_LumberMills_IndianCreekTrestle.tif"));
        SourceFilesInfo info = service.loadMappings();
        assertAddToMappingPresent(info, "test-00001", "", testSourcePaths);
        assertAddToMappingPresent(info, "test-00002", "", testSourcePaths);
        assertEquals(2, info.getMappings().size());
    }

    @Test
    public void addToMappingMultipleExtensionsTest() throws Exception {
        AddSourceFileMappingOptions options = new AddSourceFileMappingOptions();
        options.setBasePath(Path.of("src/test/resources/files"));
        options.setExtensions(Arrays.asList("tif", "jpeg"));
        options.setOptionalIdPrefix("test");

        service.addToMapping(options);

        List<Path> testSourcePaths = Arrays.asList(Path.of("D2_035_Varners_DrugStore_interior.tif"),
                Path.of("IMG_2377.jpeg"), Path.of("MJM_7_016_LumberMills_IndianCreekTrestle.tif"));
        SourceFilesInfo info = service.loadMappings();
        assertAddToMappingPresent(info, "test-00001", "", testSourcePaths);
        assertAddToMappingPresent(info, "test-00002", "", testSourcePaths);
        assertAddToMappingPresent(info, "test-00003", "", testSourcePaths);
        assertEquals(3, info.getMappings().size());
    }

    @Test
    public void rerunAddToMappingNewFileAddedTest() throws Exception {
        AddSourceFileMappingOptions options = new AddSourceFileMappingOptions();
        options.setBasePath(Path.of("src/test/resources/files"));
        options.setExtensions(Collections.singletonList("tif"));
        options.setOptionalIdPrefix("test");

        service.addToMapping(options);

        SourceFilesInfo info = service.loadMappings();
        List<Path> testSourcePaths = Arrays.asList(Path.of("D2_035_Varners_DrugStore_interior.tif"),
                Path.of("MJM_7_016_LumberMills_IndianCreekTrestle.tif"));
        assertAddToMappingPresent(info, "test-00001", "", testSourcePaths);
        assertAddToMappingPresent(info, "test-00002", "", testSourcePaths);
        assertEquals(2, info.getMappings().size());

        AddSourceFileMappingOptions options2 = new AddSourceFileMappingOptions();
        options2.setBasePath(Path.of("src/test/resources/files"));
        options2.setExtensions(Collections.singletonList("jpeg"));
        options2.setOptionalIdPrefix("test");
        // Do not include update flag and files are still added

        service.addToMapping(options2);

        List<Path> testSourcePaths2 = Arrays.asList(Path.of("D2_035_Varners_DrugStore_interior.tif"),
                Path.of("IMG_2377.jpeg"), Path.of("MJM_7_016_LumberMills_IndianCreekTrestle.tif"));
        SourceFilesInfo info2 = service.loadMappings();
        assertAddToMappingPresent(info2, "test-00001", "", testSourcePaths2);
        assertAddToMappingPresent(info2, "test-00002", "", testSourcePaths2);
        assertAddToMappingPresent(info2, "test-00003", "", testSourcePaths2);
        assertEquals(3, info2.getMappings().size());
    }

    @Test
    public void rerunAddToMappingNoFilesAddedTest() throws Exception {
        AddSourceFileMappingOptions options = new AddSourceFileMappingOptions();
        options.setBasePath(Path.of("src/test/resources/files"));
        options.setExtensions(Collections.singletonList("tif"));
        options.setOptionalIdPrefix("test");

        service.addToMapping(options);

        List<Path> testSourcePaths = Arrays.asList(Path.of("D2_035_Varners_DrugStore_interior.tif"),
                Path.of("MJM_7_016_LumberMills_IndianCreekTrestle.tif"));
        SourceFilesInfo info = service.loadMappings();
        assertAddToMappingPresent(info, "test-00001", "", testSourcePaths);
        assertAddToMappingPresent(info, "test-00002", "", testSourcePaths);
        assertEquals(2, info.getMappings().size());

        AddSourceFileMappingOptions options2 = new AddSourceFileMappingOptions();
        options2.setBasePath(Path.of("src/test/resources/files"));
        options2.setExtensions(Collections.singletonList("tif"));
        options2.setOptionalIdPrefix("test");

        service.addToMapping(options2);

        List<Path> testSourcePaths2 = Arrays.asList(Path.of("D2_035_Varners_DrugStore_interior.tif"),
                Path.of("MJM_7_016_LumberMills_IndianCreekTrestle.tif"));
        SourceFilesInfo info2 = service.loadMappings();
        assertAddToMappingPresent(info2, "test-00001", "", testSourcePaths2);
        assertAddToMappingPresent(info2, "test-00002", "", testSourcePaths2);
        assertEquals(2, info2.getMappings().size());
    }

    @Test
    public void addToMappingParseLettersInIdTest() throws Exception {
        writeCsv(mappingBody("test-0000B,," + Path.of("IMG_2377.jpeg") + ","));

        AddSourceFileMappingOptions options = new AddSourceFileMappingOptions();
        options.setBasePath(Path.of("src/test/resources/files"));
        options.setExtensions(Collections.singletonList("tif"));
        options.setOptionalIdPrefix("test");

        service.addToMapping(options);

        List<Path> testSourcePaths2 = Arrays.asList(Path.of("D2_035_Varners_DrugStore_interior.tif"),
                Path.of("IMG_2377.jpeg"), Path.of("MJM_7_016_LumberMills_IndianCreekTrestle.tif"));
        SourceFilesInfo info2 = service.loadMappings();
        assertAddToMappingPresent(info2, "test-0000B", "", testSourcePaths2);
        assertAddToMappingPresent(info2, "test-0000C", "", testSourcePaths2);
        assertAddToMappingPresent(info2, "test-0000D", "", testSourcePaths2);
        assertEquals(3, info2.getMappings().size());
    }

    private void assertMappingPresent(SourceFilesInfo info, String cdmid, String matchingVal, Path sourcePath,
                                      Path... potentialPaths) {
        List<SourceFileMapping> mappings = info.getMappings();
        SourceFileMapping mapping = mappings.stream().filter(m -> m.getCdmId().equals(cdmid)).findFirst().get();

        assertEquals(sourcePath, mapping.getFirstSourcePath());
        assertEquals(matchingVal, mapping.getMatchingValue());
        if (potentialPaths.length > 0) {
            for (Path potentialPath : potentialPaths) {
                assertTrue(mapping.getPotentialMatches().contains(potentialPath.toString()),
                        "Mapping did not contain expected potential path: " + potentialPath);
            }
        }
    }

    private void assertAddToMappingPresent(SourceFilesInfo info, String cdmid, String matchingVal, List<Path> sourcePaths) {
        List<SourceFileMapping> mappings = info.getMappings();
        SourceFileMapping mapping = mappings.stream().filter(m -> m.getCdmId().equals(cdmid)).findFirst().get();

        assertTrue(sourcePaths.contains(mapping.getFirstSourcePath()));
        assertEquals(matchingVal, mapping.getMatchingValue());
    }

    private GenerateSourceFileMappingOptions makeDefaultOptions() {
        GenerateSourceFileMappingOptions options = new GenerateSourceFileMappingOptions();
        options.setBasePath(basePath);
        options.setExportField("file");
        options.setFieldMatchingPattern("(.+)");
        options.setFilenameTemplate("$1");
        return options;
    }

    private void assertExceptionContains(String expected, Exception e) {
        assertTrue(e.getMessage().contains(expected),
                "Expected message exception to contain '" + expected + "', but was: " + e.getMessage());
    }

    private void setIndexedDate() throws Exception {
        project.getProjectProperties().setIndexedDate(Instant.now());
        ProjectPropertiesSerialization.write(project);
    }

    private void assertMappedDatePresent() throws Exception {
        MigrationProjectProperties props = ProjectPropertiesSerialization.read(project.getProjectPropertiesPath());
        assertNotNull(props.getSourceFilesUpdatedDate());
    }

    private void assertMappedDateNotPresent() throws Exception {
        MigrationProjectProperties props = ProjectPropertiesSerialization.read(project.getProjectPropertiesPath());
        assertNull(props.getSourceFilesUpdatedDate());
    }

    private String mappingBody(String... rows) {
        return String.join(",", SourceFilesInfo.CSV_HEADERS) + "\n"
                + String.join("\n", rows);
    }

    private void writeCsv(String mappingBody) throws IOException {
        FileUtils.write(project.getSourceFilesMappingPath().toFile(),
                mappingBody, StandardCharsets.UTF_8);
    }
}
