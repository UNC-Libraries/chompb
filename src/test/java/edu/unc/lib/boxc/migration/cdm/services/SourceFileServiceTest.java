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
package edu.unc.lib.boxc.migration.cdm.services;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;

import edu.unc.lib.boxc.migration.cdm.test.OutputHelper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProjectProperties;
import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo;
import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo.SourceFileMapping;
import edu.unc.lib.boxc.migration.cdm.options.SourceFileMappingOptions;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;

/**
 * @author bbpennel
 */
public class SourceFileServiceTest {
    private static final String PROJECT_NAME = "proj";

    @Rule
    public final TemporaryFolder tmpFolder = new TemporaryFolder();

    private MigrationProject project;
    private CdmIndexService indexService;
    private CdmFieldService fieldService;
    private SourceFileService service;

    private Path basePath;

    @Before
    public void setup() throws Exception {
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder.getRoot().toPath(), PROJECT_NAME, null, "user");
        Files.createDirectories(project.getExportPath());

        fieldService = new CdmFieldService();
        indexService = new CdmIndexService();
        indexService.setProject(project);
        indexService.setFieldService(fieldService);
        service = new SourceFileService();
        service.setIndexService(indexService);
        service.setProject(project);

        basePath = tmpFolder.newFolder().toPath();
    }

    @Test
    public void generateNoIndexTest() throws Exception {
        SourceFileMappingOptions options = makeDefaultOptions();

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
        SourceFileMappingOptions options = makeDefaultOptions();
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
        SourceFileMappingOptions options = makeDefaultOptions();
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
        indexExportSamples();
        SourceFileMappingOptions options = makeDefaultOptions();
        service.generateMapping(options);

        SourceFilesInfo info = service.loadMappings();
        assertMappingPresent(info, "25", "276_182_E.tif", null);
        assertMappingPresent(info, "26", "276_183B_E.tif", null);
        assertMappingPresent(info, "27", "276_203_E.tif", null);

        assertMappedDatePresent();
    }

    @Test
    public void generateExactMatchTest() throws Exception {
        indexExportSamples();
        SourceFileMappingOptions options = makeDefaultOptions();
        Path srcPath1 = addSourceFile("276_182_E.tif");

        service.generateMapping(options);

        SourceFilesInfo info = service.loadMappings();
        assertMappingPresent(info, "25", "276_182_E.tif", srcPath1);
        assertMappingPresent(info, "26", "276_183B_E.tif", null);
        assertMappingPresent(info, "27", "276_203_E.tif", null);

        assertMappedDatePresent();
    }

    @Test
    public void generateTransformedMatchTest() throws Exception {
        indexExportSamples();
        SourceFileMappingOptions options = makeDefaultOptions();
        options.setFieldMatchingPattern("(\\d+)\\_([^_]+)\\_E.tif");
        options.setFilenameTemplate("00$1_op0$2_0001_e.tif");
        Path srcPath1 = addSourceFile("00276_op0182_0001_e.tif");
        Path srcPath3 = addSourceFile("00276_op0203_0001_e.tif");

        service.generateMapping(options);

        SourceFilesInfo info = service.loadMappings();
        assertMappingPresent(info, "25", "276_182_E.tif", srcPath1);
        assertMappingPresent(info, "26", "276_183B_E.tif", null);
        assertMappingPresent(info, "27", "276_203_E.tif", srcPath3);

        assertMappedDatePresent();
    }

    @Test
    public void generateNestedMatchesTest() throws Exception {
        indexExportSamples();
        SourceFileMappingOptions options = makeDefaultOptions();
        options.setPathPattern("**/*.tif");
        Path srcPath1 = addSourceFile("nested/path/276_182_E.tif");
        Path srcPath2 = addSourceFile("276_183B_E.tif");

        service.generateMapping(options);

        SourceFilesInfo info = service.loadMappings();
        assertMappingPresent(info, "25", "276_182_E.tif", srcPath1);
        assertMappingPresent(info, "26", "276_183B_E.tif", srcPath2);
        assertMappingPresent(info, "27", "276_203_E.tif", null);

        assertMappedDatePresent();
    }

    @Test
    public void generateTransformedNestedMatchTest() throws Exception {
        indexExportSamples();
        SourceFileMappingOptions options = makeDefaultOptions();
        options.setFieldMatchingPattern("(\\d+)\\_([^_]+)\\_E.tif");
        options.setFilenameTemplate("00$1_op0$2_0001_e.tif");
        Path srcPath1 = addSourceFile("nested/path/00276_op0182_0001_e.tif");
        // Add extra file that does not match the pattern
        addSourceFile("nested/path/276_182_E.tif");

        service.generateMapping(options);

        SourceFilesInfo info = service.loadMappings();
        assertMappingPresent(info, "25", "276_182_E.tif", srcPath1);
        assertMappingPresent(info, "26", "276_183B_E.tif", null);
        assertMappingPresent(info, "27", "276_203_E.tif", null);

        assertMappedDatePresent();
    }

    @Test
    public void generateMultipleMatchesForSameObjectTest() throws Exception {
        indexExportSamples();
        SourceFileMappingOptions options = makeDefaultOptions();
        options.setPathPattern("**/*.tif");
        Path srcPath1 = addSourceFile("nested/path/276_182_E.tif");
        Path srcPath1Dupe = addSourceFile("nested/276_182_E.tif");
        Path srcPath2 = addSourceFile("276_183B_E.tif");

        service.generateMapping(options);

        SourceFilesInfo info = service.loadMappings();
        assertMappingPresent(info, "25", "276_182_E.tif", null, srcPath1, srcPath1Dupe);
        assertMappingPresent(info, "26", "276_183B_E.tif", srcPath2);
        assertMappingPresent(info, "27", "276_203_E.tif", null);

        assertMappedDatePresent();
    }

    @Test
    public void generateDryRunTest() throws Exception {
        OutputHelper.captureOutput(() -> {
            indexExportSamples();
            SourceFileMappingOptions options = makeDefaultOptions();
            options.setDryRun(true);
            addSourceFile("276_182_E.tif");

            service.generateMapping(options);

            assertFalse(Files.exists(project.getSourceFilesMappingPath()));

            assertMappedDateNotPresent();
        });
    }

    @Test
    public void generateMappingExistsTest() throws Exception {
        indexExportSamples();
        SourceFileMappingOptions options = makeDefaultOptions();
        Path srcPath1 = addSourceFile("276_182_E.tif");

        service.generateMapping(options);

        SourceFilesInfo info = service.loadMappings();
        assertMappingPresent(info, "25", "276_182_E.tif", srcPath1);
        assertMappingPresent(info, "26", "276_183B_E.tif", null);
        assertMappingPresent(info, "27", "276_203_E.tif", null);

        // Add extra matching source file, which should not show up during rejected run
        Path srcPath2 = addSourceFile("276_183B_E.tif");
        try {
            service.generateMapping(options);
            fail();
        } catch (MigrationException e) {
            assertTrue(e.getMessage().contains("Cannot create mapping, a file already exists"));
        }

        SourceFilesInfo info2 = service.loadMappings();
        assertMappingPresent(info2, "25", "276_182_E.tif", srcPath1);
        assertMappingPresent(info2, "26", "276_183B_E.tif", null);
        assertMappingPresent(info2, "27", "276_203_E.tif", null);

        // Try again with force
        options.setForce(true);
        service.generateMapping(options);

        SourceFilesInfo info3 = service.loadMappings();
        assertMappingPresent(info3, "25", "276_182_E.tif", srcPath1);
        assertMappingPresent(info3, "26", "276_183B_E.tif", srcPath2);
        assertMappingPresent(info3, "27", "276_203_E.tif", null);

        assertMappedDatePresent();
    }

    @Test
    public void generateMatchesLowercaseTest() throws Exception {
        indexExportSamples();
        SourceFileMappingOptions options = makeDefaultOptions();
        options.setLowercaseTemplate(true);
        Path srcPath1 = addSourceFile("276_182_e.tif");
        Path srcPath2 = addSourceFile("276_183b_e.tif");

        service.generateMapping(options);

        SourceFilesInfo info = service.loadMappings();
        assertMappingPresent(info, "25", "276_182_E.tif", srcPath1);
        assertMappingPresent(info, "26", "276_183B_E.tif", srcPath2);
        assertMappingPresent(info, "27", "276_203_E.tif", null);

        assertMappedDatePresent();
    }

    @Test
    public void generateUpdateNoExistingFileTest() throws Exception {
        indexExportSamples();
        SourceFileMappingOptions options = makeDefaultOptions();
        options.setUpdate(true);
        Path srcPath1 = addSourceFile("276_182_E.tif");

        service.generateMapping(options);

        SourceFilesInfo info = service.loadMappings();
        assertMappingPresent(info, "25", "276_182_E.tif", srcPath1);
        assertMappingPresent(info, "26", "276_183B_E.tif", null);
        assertMappingPresent(info, "27", "276_203_E.tif", null);

        assertMappedDatePresent();
    }

    @Test
    public void generateUpdateExistingNoChangesTest() throws Exception {
        indexExportSamples();
        SourceFileMappingOptions options = makeDefaultOptions();
        Path srcPath1 = addSourceFile("276_182_E.tif");

        service.generateMapping(options);

        SourceFilesInfo info = service.loadMappings();
        assertMappingPresent(info, "25", "276_182_E.tif", srcPath1);
        assertMappingPresent(info, "26", "276_183B_E.tif", null);
        assertMappingPresent(info, "27", "276_203_E.tif", null);

        assertMappedDatePresent();

        options.setUpdate(true);
        service.generateMapping(options);

        SourceFilesInfo info2 = service.loadMappings();
        assertMappingPresent(info2, "25", "276_182_E.tif", srcPath1);
        assertMappingPresent(info2, "26", "276_183B_E.tif", null);
        assertMappingPresent(info2, "27", "276_203_E.tif", null);
    }

    @Test
    public void generateUpdateAddNewSourceFileTest() throws Exception {
        indexExportSamples();
        SourceFileMappingOptions options = makeDefaultOptions();
        Path srcPath1 = addSourceFile("276_182_E.tif");

        service.generateMapping(options);

        Path srcPath2 = addSourceFile("276_183B_E.tif");
        options.setUpdate(true);
        service.generateMapping(options);

        SourceFilesInfo info2 = service.loadMappings();
        assertMappingPresent(info2, "25", "276_182_E.tif", srcPath1);
        assertMappingPresent(info2, "26", "276_183B_E.tif", srcPath2);
        assertMappingPresent(info2, "27", "276_203_E.tif", null);
    }

    @Test
    public void generateUpdateChangeSourceFileTest() throws Exception {
        indexExportSamples();
        SourceFileMappingOptions options = makeDefaultOptions();
        options.setPathPattern("**/*");
        Path srcPath1 = addSourceFile("276_182_E.tif");

        service.generateMapping(options);

        Files.delete(srcPath1);
        addSourceFile("nested/276_182_E.tif");
        options.setUpdate(true);
        service.generateMapping(options);

        SourceFilesInfo info2 = service.loadMappings();
        // Should still list the original source path
        assertMappingPresent(info2, "25", "276_182_E.tif", srcPath1);
        assertMappingPresent(info2, "26", "276_183B_E.tif", null);
        assertMappingPresent(info2, "27", "276_203_E.tif", null);
    }

    @Test
    public void generateUpdateChangeSourceFileWithForceTest() throws Exception {
        indexExportSamples();
        SourceFileMappingOptions options = makeDefaultOptions();
        options.setPathPattern("**/*");
        Path srcPath1 = addSourceFile("276_182_E.tif");

        service.generateMapping(options);

        Files.delete(srcPath1);
        Path srcPath2 = addSourceFile("nested/276_182_E.tif");
        options.setUpdate(true);
        options.setForce(true);
        service.generateMapping(options);

        SourceFilesInfo info2 = service.loadMappings();
        // Should be using the new path
        assertMappingPresent(info2, "25", "276_182_E.tif", srcPath2);
        assertMappingPresent(info2, "26", "276_183B_E.tif", null);
        assertMappingPresent(info2, "27", "276_203_E.tif", null);
    }

    @Test
    public void generateUpdateAddPotentialMatchToExistingMatchTest() throws Exception {
        indexExportSamples();
        SourceFileMappingOptions options = makeDefaultOptions();
        options.setPathPattern("**/*");
        Path srcPath1 = addSourceFile("276_182_E.tif");

        service.generateMapping(options);

        addSourceFile("nested/276_182_E.tif");
        options.setUpdate(true);
        service.generateMapping(options);

        SourceFilesInfo info2 = service.loadMappings();
        // Should be using the new path
        assertMappingPresent(info2, "25", "276_182_E.tif", srcPath1);
        assertMappingPresent(info2, "26", "276_183B_E.tif", null);
        assertMappingPresent(info2, "27", "276_203_E.tif", null);
    }

    @Test
    public void generateUpdateAddPotentialMatchTest() throws Exception {
        indexExportSamples();
        SourceFileMappingOptions options = makeDefaultOptions();
        options.setPathPattern("**/*");

        service.generateMapping(options);
        SourceFilesInfo info = service.loadMappings();
        assertMappingPresent(info, "25", "276_182_E.tif", null);
        assertMappingPresent(info, "26", "276_183B_E.tif", null);
        assertMappingPresent(info, "27", "276_203_E.tif", null);

        Path srcPath1 = addSourceFile("276_182_E.tif");
        Path srcPath2 = addSourceFile("nested/276_182_E.tif");
        options.setUpdate(true);
        service.generateMapping(options);

        SourceFilesInfo info2 = service.loadMappings();
        // Should be using the new path
        assertMappingPresent(info2, "25", "276_182_E.tif", null, srcPath1, srcPath2);
        assertMappingPresent(info2, "26", "276_183B_E.tif", null);
        assertMappingPresent(info2, "27", "276_203_E.tif", null);
    }

    @Test
    public void generateUpdateAddPotentialMatchWithExistingPotentialTest() throws Exception {
        indexExportSamples();
        SourceFileMappingOptions options = makeDefaultOptions();
        options.setPathPattern("**/*");

        Path srcPath1 = addSourceFile("276_182_E.tif");
        Path srcPath2 = addSourceFile("nested/276_182_E.tif");

        service.generateMapping(options);
        SourceFilesInfo info = service.loadMappings();
        assertMappingPresent(info, "25", "276_182_E.tif", null, srcPath1, srcPath2);
        assertMappingPresent(info, "26", "276_183B_E.tif", null);
        assertMappingPresent(info, "27", "276_203_E.tif", null);

        Path srcPath3 = addSourceFile("another/276_182_E.tif");
        options.setUpdate(true);
        service.generateMapping(options);

        SourceFilesInfo info2 = service.loadMappings();
        // Should be using the new path
        assertMappingPresent(info2, "25", "276_182_E.tif", null, srcPath1, srcPath2, srcPath3);
        assertMappingPresent(info2, "26", "276_183B_E.tif", null);
        assertMappingPresent(info2, "27", "276_203_E.tif", null);
    }

    @Test
    public void generateUpdateAddNewRecordsTest() throws Exception {
        indexExportSamples();
        SourceFileMappingOptions options = makeDefaultOptions();
        Path srcPath1 = addSourceFile("276_182_E.tif");

        service.generateMapping(options);

        // Add more exported objects
        Files.copy(Paths.get("src/test/resources/sample_exports/export_2.xml"),
                project.getExportPath().resolve("export_2.xml"));
        indexService.createDatabase(true);
        indexService.indexAll();

        Path srcPath2 = addSourceFile("276_245a_E.tif");
        options.setUpdate(true);
        service.generateMapping(options);

        SourceFilesInfo info2 = service.loadMappings();
        assertMappingPresent(info2, "25", "276_182_E.tif", srcPath1);
        assertMappingPresent(info2, "26", "276_183B_E.tif", null);
        assertMappingPresent(info2, "27", "276_203_E.tif", null);
        assertMappingPresent(info2, "28", "276_241_E.tif", null);
        assertMappingPresent(info2, "29", "276_245a_E.tif", srcPath2);
    }

    @Test
    public void generateUpdateDryRunAddNewSourceFileTest() throws Exception {
        indexExportSamples();
        SourceFileMappingOptions options = makeDefaultOptions();
        Path srcPath1 = addSourceFile("276_182_E.tif");

        service.generateMapping(options);

        OutputHelper.captureOutput(() -> {
            addSourceFile("276_183B_E.tif");
            options.setUpdate(true);
            options.setDryRun(true);
            service.generateMapping(options);

            SourceFilesInfo info2 = service.loadMappings();
            assertMappingPresent(info2, "25", "276_182_E.tif", srcPath1);
            // Mapping should be unchanged
            assertMappingPresent(info2, "26", "276_183B_E.tif", null);
            assertMappingPresent(info2, "27", "276_203_E.tif", null);
        });
    }

    private void assertMappingPresent(SourceFilesInfo info, String cdmid, String matchingVal, Path sourcePath,
            Path... potentialPaths) {
        List<SourceFileMapping> mappings = info.getMappings();
        SourceFileMapping mapping = mappings.stream().filter(m -> m.getCdmId().equals(cdmid)).findFirst().get();

        assertEquals(sourcePath, mapping.getSourcePath());
        assertEquals(matchingVal, mapping.getMatchingValue());
        if (potentialPaths.length > 0) {
            for (Path potentialPath : potentialPaths) {
                assertTrue("Mapping did not contain expected potential path: " + potentialPath,
                        mapping.getPotentialMatches().contains(potentialPath.toString()));
            }
        }
    }

    private Path addSourceFile(String relPath) throws IOException {
        Path srcPath = basePath.resolve(relPath);
        // Create parent directories in case they don't exist
        Files.createDirectories(srcPath.getParent());
        Files.createFile(srcPath);
        return srcPath;
    }

    private SourceFileMappingOptions makeDefaultOptions() {
        SourceFileMappingOptions options = new SourceFileMappingOptions();
        options.setBasePath(basePath);
        options.setExportField("file");
        options.setFieldMatchingPattern("(.+)");
        options.setFilenameTemplate("$1");
        return options;
    }

    private void indexExportSamples() throws Exception {
        Files.copy(Paths.get("src/test/resources/sample_exports/export_1.xml"),
                project.getExportPath().resolve("export_all.xml"));
        Files.copy(Paths.get("src/test/resources/gilmer_fields.csv"), project.getFieldsPath());

        project.getProjectProperties().setExportedDate(Instant.now());
        indexService.createDatabase(true);
        indexService.indexAll();
    }

    private void assertExceptionContains(String expected, Exception e) {
        assertTrue("Expected message exception to contain '" + expected + "', but was: " + e.getMessage(),
                e.getMessage().contains(expected));
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
}
