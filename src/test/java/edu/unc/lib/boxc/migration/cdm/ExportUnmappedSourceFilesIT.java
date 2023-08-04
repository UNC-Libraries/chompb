package edu.unc.lib.boxc.migration.cdm;

import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.options.SourceFileMappingOptions;
import edu.unc.lib.boxc.migration.cdm.services.CdmFileRetrievalService;
import edu.unc.lib.boxc.migration.cdm.services.CdmIndexService;
import edu.unc.lib.boxc.migration.cdm.test.TestSshServer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * @author bbpennel
 */
public class ExportUnmappedSourceFilesIT extends AbstractCommandIT {
    private TestSshServer testSshServer;
    private Path basePath;

    @BeforeEach
    public void setUp() throws Exception {
        defaultCollectionId = "mini_gilmer";
        testSshServer = new TestSshServer();
        testSshServer.startServer();
        setupChompbConfig();
        initProjectAndHelper();
        testHelper.indexExportData("mini_gilmer");
        basePath = tmpFolder;
    }

    @AfterEach
    public void cleanup() throws Exception {
        testSshServer.stopServer();
    }

    private String[] exportArgs(String... extras) {
        String[] defaultArgs = new String[] {
                "-w", project.getProjectPath().toString(),
                "--env-config", chompbConfigPath,
                "source_files", "export_unmapped",
                "-p", TestSshServer.PASSWORD};
        return ArrayUtils.addAll(defaultArgs, extras);
    }

    @Test
    public void sourceMappingNotGeneratedTest() throws Exception {
        String[] args = exportArgs();
        executeExpectFailure(args);

        assertFalse(Files.exists(CdmFileRetrievalService.getExportedSourceFilesPath(project)),
                "Export dir should not be created");
        assertOutputContains("Source files must be mapped");
    }

    @Test
    public void withAllFilesMappedTest() throws Exception {
        testHelper.populateSourceFiles("276_182_E.tif", "276_183_E.tif", "276_203_E.tif");
        SourceFileMappingOptions opts = testHelper.makeSourceFileOptions(testHelper.getSourceFilesBasePath());
        testHelper.getSourceFileService().generateMapping(opts);
        var sourceMappingPath = project.getSourceFilesMappingPath();
        var originalContents = FileUtils.readFileToString(sourceMappingPath.toFile(), StandardCharsets.UTF_8);

        String[] args = exportArgs();
        executeExpectSuccess(args);

        var updatedContents = FileUtils.readFileToString(sourceMappingPath.toFile(), StandardCharsets.UTF_8);
        assertEquals(originalContents, updatedContents, "Mapping contents must be unchanged");
    }

    @Test
    public void withUnmappedFilesTest() throws Exception {
        var localSourcePaths = testHelper.populateSourceFiles("276_183_E.tif");
        SourceFileMappingOptions opts = testHelper.makeSourceFileOptions(testHelper.getSourceFilesBasePath());
        var sourceFileService = testHelper.getSourceFileService();
        sourceFileService.generateMapping(opts);
        var sourceMappingPath = project.getSourceFilesMappingPath();
        var originalContents = FileUtils.readFileToString(sourceMappingPath.toFile(), StandardCharsets.UTF_8);

        String[] args = exportArgs();
        executeExpectSuccess(args);

        var updatedContents = FileUtils.readFileToString(sourceMappingPath.toFile(), StandardCharsets.UTF_8);
        assertNotEquals("Mapping contents must be changed", originalContents, updatedContents);

        var exportedSourceFilesPath = CdmFileRetrievalService.getExportedSourceFilesPath(project);
        var mappingInfo = sourceFileService.loadMappings();
        var mapping1 = mappingInfo.getMappingByCdmId("25");
        assertEquals(exportedSourceFilesPath.resolve("26.JP2"), mapping1.getFirstSourcePath());
        var mapping2 = mappingInfo.getMappingByCdmId("26");
        assertEquals(localSourcePaths.get(0), mapping2.getFirstSourcePath());
        var mapping3 = mappingInfo.getMappingByCdmId("27");
        assertEquals(exportedSourceFilesPath.resolve("50.jp2"), mapping3.getFirstSourcePath());
    }

    @Test
    public void withMissingUnmappedFilesTest() throws Exception {
        var localSourcePaths = testHelper.populateSourceFiles("276_183_E.tif");
        SourceFileMappingOptions opts = testHelper.makeSourceFileOptions(testHelper.getSourceFilesBasePath());
        var sourceFileService = testHelper.getSourceFileService();
        sourceFileService.generateMapping(opts);
        var sourceMappingPath = project.getSourceFilesMappingPath();
        var originalContents = FileUtils.readFileToString(sourceMappingPath.toFile(), StandardCharsets.UTF_8);

        // Change the filename of one of the records so it doesn't match any existing files
        try (var conn = testHelper.getIndexService().openDbConnection()) {
            var stmt = conn.createStatement();
            stmt.executeUpdate("UPDATE " + CdmIndexService.TB_NAME + " SET find = '25.JP2' WHERE "
                    + CdmFieldInfo.CDM_ID + " = 25");
        }

        String[] args = exportArgs();
        // Expect a partial failure, with some items updated and others not
        executeExpectFailure(args);

        var updatedContents = FileUtils.readFileToString(sourceMappingPath.toFile(), StandardCharsets.UTF_8);
        assertNotEquals("Mapping contents must be changed", originalContents, updatedContents);

        var exportedSourceFilesPath = CdmFileRetrievalService.getExportedSourceFilesPath(project);
        var mappingInfo = sourceFileService.loadMappings();
        var mapping1 = mappingInfo.getMappingByCdmId("25");
        assertNull(mapping1.getSourcePaths(), "Mapping for resource with missing file must be null");
        var mapping2 = mappingInfo.getMappingByCdmId("26");
        assertEquals(localSourcePaths.get(0), mapping2.getFirstSourcePath());
        var mapping3 = mappingInfo.getMappingByCdmId("27");
        assertEquals(exportedSourceFilesPath.resolve("50.jp2"), mapping3.getFirstSourcePath());
    }

    @Test
    public void authenticationFailureTest() throws Exception {
        testHelper.populateSourceFiles("276_182_E.tif", "276_183_E.tif", "276_203_E.tif");
        SourceFileMappingOptions opts = testHelper.makeSourceFileOptions(testHelper.getSourceFilesBasePath());
        testHelper.getSourceFileService().generateMapping(opts);
        var sourceMappingPath = project.getSourceFilesMappingPath();
        var originalContents = FileUtils.readFileToString(sourceMappingPath.toFile(), StandardCharsets.UTF_8);

        String[] args = exportArgs();
        args[args.length - 1] = "nope";
        executeExpectFailure(args);

        assertOutputContains("Authentication to server failed");

        var updatedContents = FileUtils.readFileToString(sourceMappingPath.toFile(), StandardCharsets.UTF_8);
        assertEquals(originalContents, updatedContents, "Mapping contents must be unchanged");
    }
}
