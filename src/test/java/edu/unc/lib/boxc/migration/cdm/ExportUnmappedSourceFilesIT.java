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

import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.options.SourceFileMappingOptions;
import edu.unc.lib.boxc.migration.cdm.services.CdmFileRetrievalService;
import edu.unc.lib.boxc.migration.cdm.services.CdmIndexService;
import edu.unc.lib.boxc.migration.cdm.test.TestSshServer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

/**
 * @author bbpennel
 */
public class ExportUnmappedSourceFilesIT extends AbstractCommandIT {
    private TestSshServer testSshServer;
    private Path basePath;

    @Before
    public void setUp() throws Exception {
        defaultCollectionId = "mini_gilmer";
        testSshServer = new TestSshServer();
        testSshServer.startServer();
        setupChompbConfig();
        initProjectAndHelper();
        testHelper.indexExportData("mini_gilmer");
        basePath = tmpFolder.newFolder().toPath();
    }

    @After
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

        assertFalse("Export dir should not be created",
                Files.exists(CdmFileRetrievalService.getExportedSourceFilesPath(project)));
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
        assertEquals("Mapping contents must be unchanged", originalContents, updatedContents);
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
        assertEquals(exportedSourceFilesPath.resolve("26.JP2"), mapping1.getSourcePath());
        var mapping2 = mappingInfo.getMappingByCdmId("26");
        assertEquals(localSourcePaths.get(0), mapping2.getSourcePath());
        var mapping3 = mappingInfo.getMappingByCdmId("27");
        assertEquals(exportedSourceFilesPath.resolve("50.jp2"), mapping3.getSourcePath());
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
        assertNull("Mapping for resource with missing file must be null", mapping1.getSourcePath());
        var mapping2 = mappingInfo.getMappingByCdmId("26");
        assertEquals(localSourcePaths.get(0), mapping2.getSourcePath());
        var mapping3 = mappingInfo.getMappingByCdmId("27");
        assertEquals(exportedSourceFilesPath.resolve("50.jp2"), mapping3.getSourcePath());
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
        assertEquals("Mapping contents must be unchanged", originalContents, updatedContents);
    }
}
