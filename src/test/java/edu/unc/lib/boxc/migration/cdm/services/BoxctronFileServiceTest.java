package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProjectProperties;
import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo;
import edu.unc.lib.boxc.migration.cdm.options.BoxctronFileMappingOptions;
import edu.unc.lib.boxc.migration.cdm.test.BxcEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.SipServiceHelper;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class BoxctronFileServiceTest {
    private static final String PROJECT_NAME = "proj";

    @TempDir
    public Path tmpFolder;

    private MigrationProject project;
    private BoxctronFileService service;
    private SipServiceHelper testHelper;

    @BeforeEach
    public void setup() throws Exception {
        project = MigrationProjectFactory.createCdmMigrationProject(
                tmpFolder, PROJECT_NAME, null, "user",
                CdmEnvironmentHelper.DEFAULT_ENV_ID, BxcEnvironmentHelper.DEFAULT_ENV_ID);
        Files.createDirectories(project.getExportPath());

        testHelper = new SipServiceHelper(project, tmpFolder);

        service = testHelper.getBoxctronFileService();
        service.setProject(project);
        service.setIndexService(testHelper.getIndexService());
    }

    @Test
    public void generateNoIndexTest() throws Exception {
        BoxctronFileMappingOptions options = new BoxctronFileMappingOptions();

        try {
            service.generateMapping(options);
            fail();
        } catch (InvalidProjectStateException e) {
            assertExceptionContains("Project must be indexed", e);
            assertMappedDateNotPresent();
        }
    }

    @Test
    public void generateBoxctronResultsDoNotExistTest() throws Exception {
        setIndexedDate();
        BoxctronFileMappingOptions options = new BoxctronFileMappingOptions();

        try {
            service.generateMapping(options);
            fail();
        } catch (NoSuchFileException e) {
            assertExceptionContains("proj/processing/results/velocicroptor/output/data.csv does not exist", e);
            assertMappedDateNotPresent();
        }
    }

    @Test
    public void generateNoSourceFilesTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        testHelper.populateSourceFiles("276_182_E.tif", "276_183_E.tif", "276_203_E.tif");
        boxctronWriteCsv(boxctronMappingBody("/mnt/projects/test_staging/mini_gilmer/276_182_E.tif,0,0.0000,,"));
        BoxctronFileMappingOptions options = new BoxctronFileMappingOptions();
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
        testHelper.populateSourceFiles("276_182_E.tif", "276_183_E.tif", "276_203_E.tif");
        Path boxctronPath1 = tmpFolder.resolve("source/276_182_E.tif");
        boxctronWriteCsv(boxctronMappingBody(boxctronPath1 + ",1,0.9,\"[0.0, 0.9, 1.0, 1.0]\","));
        BoxctronFileMappingOptions options = new BoxctronFileMappingOptions();

        service.generateMapping(options);

        SourceFilesInfo info = service.loadMappings();
        Path accessPath1 = project.getProjectPath().resolve("processing/results/velocicroptor/output" + boxctronPath1 + ".jpg");
        assertMappingPresent(info, "25", "276_182_E.tif", accessPath1);
        assertMappingPresent(info, "26", "276_183_E.tif", null);
        assertMappingPresent(info, "27", "276_203_E.tif", null);

        assertMappedDatePresent();
    }

    @Test
    public void generateDryRunSummaryTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        testHelper.populateSourceFiles("276_182_E.tif", "276_183_E.tif", "276_203_E.tif");
        Path boxctronPath1 = tmpFolder.resolve("source/276_182_E.tif");
        boxctronWriteCsv(boxctronMappingBody(boxctronPath1 + ",1,0.9,\"[0.0, 0.9, 1.0, 1.0]\","));
        BoxctronFileMappingOptions options = new BoxctronFileMappingOptions();
        options.setDryRun(true);

        service.generateMapping(options);

        assertFalse(Files.exists(project.getAccessFilesMappingPath()));
        assertMappedDateNotPresent();
    }

    @Test
    public void generateUpdateNoExistingFileTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        testHelper.populateSourceFiles("276_182_E.tif", "276_183_E.tif", "276_203_E.tif");
        Path boxctronPath1 = tmpFolder.resolve("source/276_182_E.tif");
        boxctronWriteCsv(boxctronMappingBody(boxctronPath1 + ",1,0.9,\"[0.0, 0.9, 1.0, 1.0]\","));
        BoxctronFileMappingOptions options = new BoxctronFileMappingOptions();
        options.setUpdate(true);

        service.generateMapping(options);

        SourceFilesInfo info = service.loadMappings();
        Path accessPath1 = project.getProjectPath().resolve("processing/results/velocicroptor/output" + boxctronPath1 + ".jpg");
        assertMappingPresent(info, "25", "276_182_E.tif", accessPath1);
        assertMappingPresent(info, "26", "276_183_E.tif", null);
        assertMappingPresent(info, "27", "276_203_E.tif", null);

        assertMappedDatePresent();
    }

    @Test
    public void generateUpdateExistingNoChangesTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        testHelper.populateSourceFiles("276_182_E.tif", "276_183_E.tif", "276_203_E.tif");
        Path boxctronPath1 = tmpFolder.resolve("source/276_182_E.tif");
        boxctronWriteCsv(boxctronMappingBody(boxctronPath1 + ",1,0.9,\"[0.0, 0.9, 1.0, 1.0]\","));
        BoxctronFileMappingOptions options = new BoxctronFileMappingOptions();
        service.generateMapping(options);

        SourceFilesInfo info = service.loadMappings();
        Path accessPath1 = project.getProjectPath().resolve("processing/results/velocicroptor/output" + boxctronPath1 + ".jpg");
        assertMappingPresent(info, "25", "276_182_E.tif", accessPath1);
        assertMappingPresent(info, "26", "276_183_E.tif", null);
        assertMappingPresent(info, "27", "276_203_E.tif", null);

        assertMappedDatePresent();

        options.setUpdate(true);
        service.generateMapping(options);

        SourceFilesInfo info2 = service.loadMappings();
        Path accessPath2 = project.getProjectPath().resolve("processing/results/velocicroptor/output" + boxctronPath1 + ".jpg");
        assertMappingPresent(info, "25", "276_182_E.tif", accessPath2);
        assertMappingPresent(info2, "26", "276_183_E.tif", null);
        assertMappingPresent(info2, "27", "276_203_E.tif", null);
    }

    @Test
    public void updateWithExistingAccessMappingTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        testHelper.populateSourceFiles("276_182_E.tif", "276_183_E.tif", "276_203_E.tif");
        List<Path> accessLocs = testHelper.populateAccessFiles("276_182_E.tif");

        SourceFilesInfo info = service.loadMappings();
        assertMappingPresent(info, "25", "276_182_E.tif", accessLocs.get(0));
        assertMappingPresent(info, "26", "276_183_E.tif", null);
        assertMappingPresent(info, "27", "276_203_E.tif", null);
        assertMappedDatePresent();

        Path boxctronPath1 = tmpFolder.resolve("source/276_203_E.tif");
        boxctronWriteCsv(boxctronMappingBody(boxctronPath1 + ",1,0.9,\"[0.0, 0.9, 1.0, 1.0]\","));
        BoxctronFileMappingOptions options = new BoxctronFileMappingOptions();
        options.setUpdate(true);
        service.generateMapping(options);

        SourceFilesInfo info2 = service.loadMappings();
        Path accessPath2 = project.getProjectPath().resolve("processing/results/velocicroptor/output" + boxctronPath1 + ".jpg");
        assertMappingPresent(info, "25", "276_182_E.tif", accessLocs.get(0));
        assertMappingPresent(info2, "26", "276_183_E.tif", null);
        assertMappingPresent(info2, "27", "276_203_E.tif", accessPath2);
    }

    private void setIndexedDate() throws Exception {
        project.getProjectProperties().setIndexedDate(Instant.now());
        ProjectPropertiesSerialization.write(project);
    }

    private void assertMappingPresent(SourceFilesInfo info, String cdmid, String matchingVal, Path sourcePath,
                                      Path... potentialPaths) {
        List<SourceFilesInfo.SourceFileMapping> mappings = info.getMappings();
        SourceFilesInfo.SourceFileMapping mapping = mappings.stream().filter(m -> m.getCdmId().equals(cdmid)).findFirst().get();

        assertEquals(sourcePath, mapping.getFirstSourcePath());
        assertEquals(matchingVal, mapping.getMatchingValue());
        if (potentialPaths.length > 0) {
            for (Path potentialPath : potentialPaths) {
                assertTrue(mapping.getPotentialMatches().contains(potentialPath.toString()),
                        "Mapping did not contain expected potential path: " + potentialPath);
            }
        }
    }

    private void assertExceptionContains(String expected, Exception e) {
        assertTrue(e.getMessage().contains(expected),
                "Expected message exception to contain '" + expected + "', but was: " + e.getMessage());
    }

    private void assertMappedDatePresent() throws Exception {
        MigrationProjectProperties props = ProjectPropertiesSerialization.read(project.getProjectPropertiesPath());
        assertNotNull(props.getAccessFilesUpdatedDate());
    }

    private void assertMappedDateNotPresent() throws Exception {
        MigrationProjectProperties props = ProjectPropertiesSerialization.read(project.getProjectPropertiesPath());
        assertNull(props.getAccessFilesUpdatedDate());
    }

    private String boxctronMappingBody(String... rows) {
        return String.join(",", BoxctronFileService.DATA_CSV_HEADERS) + "\n"
                + String.join("\n", rows);
    }

    private void boxctronWriteCsv(String boxctronMappingBody) throws IOException {
        FileUtils.write(project.getVelocicroptorDataPath().toFile(),
                boxctronMappingBody, StandardCharsets.UTF_8);
    }
}
