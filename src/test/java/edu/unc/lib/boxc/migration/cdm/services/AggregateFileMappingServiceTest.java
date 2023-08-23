package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo;
import edu.unc.lib.boxc.migration.cdm.options.AggregateFileMappingOptions;
import edu.unc.lib.boxc.migration.cdm.options.GroupMappingOptions;
import edu.unc.lib.boxc.migration.cdm.options.GroupMappingSyncOptions;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.SipServiceHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author bbpennel
 */
public class AggregateFileMappingServiceTest {
    private static final String PROJECT_NAME = "proj";

    @TempDir
    public Path tmpFolder;
    private Path basePath;
    private AggregateFileMappingService service;
    private MigrationProject project;
    private SipServiceHelper testHelper;

    @BeforeEach
    public void init() throws Exception {
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder, PROJECT_NAME, null, "user", CdmEnvironmentHelper.DEFAULT_ENV_ID);
        Files.createDirectories(project.getExportPath());

        basePath = tmpFolder.resolve("testFolder");
        Files.createDirectory(basePath);
        testHelper = new SipServiceHelper(project, basePath);

        service = testHelper.getAggregateFileMappingService();
    }

    @Test
    public void generateNoIndexTest() throws Exception {
        var e = assertThrows(InvalidProjectStateException.class, () -> {
            service.generateMapping(makeDefaultOptions());
        });
        assertExceptionContains("Project must be indexed", e);
        assertMappingFileNotCreated();
    }

    @Test
    public void generateNoAggregateFilesToMapTest() throws Exception {
        testHelper.indexExportData("grouped_gilmer");
        setupGroupedIndex();
        var options = makeDefaultOptions();
        service.generateMapping(options);

        SourceFilesInfo info = service.loadMappings();
        assertMappingPresent(info, "grp:groupa:group1", "group1", null);
        assertEquals(1, info.getMappings().size());
    }

    @Test
    public void generateWithMatchTest() throws Exception {
        testHelper.indexExportData("grouped_gilmer");
        setupGroupedIndex();
        Path aggrPath1 = testHelper.addSourceFile("group1.pdf");

        var options = makeDefaultOptions();
        service.generateMapping(options);

        // Only the top mapping should exist
        assertTrue(Files.exists(project.getAggregateTopMappingPath()));
        assertFalse(Files.exists(project.getAggregateBottomMappingPath()));

        SourceFilesInfo info = service.loadMappings();
        assertMappingPresent(info, "grp:groupa:group1", "group1", aggrPath1);
        assertEquals(1, info.getMappings().size());
    }

    @Test
    public void generateWithMultipleMatchesTest() throws Exception {
        testHelper.indexExportData("grouped_gilmer");
        setupGroupedIndex();
        Path aggrPath1 = testHelper.addSourceFile("group1.pdf");
        Path aggrPath2 = testHelper.addSourceFile("access/group1.pdf");

        var options = makeDefaultOptions();
        service.generateMapping(options);

        SourceFilesInfo info = service.loadMappings();
        assertMappingPresentWithPotentials(info, "grp:groupa:group1", "group1", aggrPath1, aggrPath2);
        assertEquals(1, info.getMappings().size());
    }

    @Test
    public void generateWithDuplicateMatchOnSecondRunTest() throws Exception {
        testHelper.indexExportData("grouped_gilmer");
        setupGroupedIndex();
        Path aggrPath1 = testHelper.addSourceFile("group1.pdf");

        var options = makeDefaultOptions();
        service.generateMapping(options);
        // Second run as an update
        options.setUpdate(true);
        service.generateMapping(options);

        SourceFilesInfo info = service.loadMappings();
        assertMappingPresent(info, "grp:groupa:group1", "group1", aggrPath1);
        assertEquals(1, info.getMappings().size());
    }

    @Test
    public void generateWithNewMatchOnSecondRunTest() throws Exception {
        testHelper.indexExportData("grouped_gilmer");
        setupGroupedIndex();
        Path aggrPath1 = testHelper.addSourceFile("group1.pdf");

        var options = makeDefaultOptions();
        service.generateMapping(options);
        // Second run as an update
        Path aggrPath2 = testHelper.addSourceFile("group1_extra.pdf");
        options.setUpdate(true);
        options.setFilenameTemplate("$1_extra.pdf");
        service.generateMapping(options);

        SourceFilesInfo info = service.loadMappings();
        assertMappingPresent(info, "grp:groupa:group1", "group1", aggrPath1, aggrPath2);
        assertEquals(1, info.getMappings().size());
    }

    @Test
    public void generateWithNewMatchOnSecondRunWithForceFlagTest() throws Exception {
        testHelper.indexExportData("grouped_gilmer");
        setupGroupedIndex();
        Path aggrPath1 = testHelper.addSourceFile("group1.pdf");

        var options = makeDefaultOptions();
        service.generateMapping(options);
        // Second run as an update
        Path aggrPath2 = testHelper.addSourceFile("group1_extra.pdf");
        options.setUpdate(true);
        options.setForce(true);
        options.setFilenameTemplate("$1_extra.pdf");
        service.generateMapping(options);

        SourceFilesInfo info = service.loadMappings();
        // Only the new path should be retained with force flag
        assertMappingPresent(info, "grp:groupa:group1", "group1", aggrPath2);
        assertEquals(1, info.getMappings().size());
    }

    @Test
    public void generateWithMatchCompoundObjectsTest() throws Exception {
        testHelper.indexExportData("mini_keepsakes");
        Path aggrPath1 = testHelper.addSourceFile("617.pdf");
        Path aggrPath2 = testHelper.addSourceFile("620.pdf");

        var options = makeDefaultOptions();
        options.setExportField("find");
        options.setFieldMatchingPattern("([^.]+)\\.cpd");
        service.generateMapping(options);

        SourceFilesInfo info = service.loadMappings();
        assertMappingPresent(info, "604", "617.cpd", aggrPath1);
        assertMappingPresent(info, "607", "620.cpd", aggrPath2);
        assertEquals(2, info.getMappings().size());
    }

    @Test
    public void generateNoMatchableObjectsTest() throws Exception {
        // Doesn't contain any compounds and no grouping
        testHelper.indexExportData("mini_gilmer");

        var options = makeDefaultOptions();
        service.generateMapping(options);

        SourceFilesInfo info = service.loadMappings();
        assertTrue(info.getMappings().isEmpty());
    }

    @Test
    public void generateWithMatchAddToBottomTest() throws Exception {
        service.setSortBottom(true);
        testHelper.indexExportData("grouped_gilmer");
        setupGroupedIndex();
        Path aggrPath1 = testHelper.addSourceFile("group1.pdf");

        var options = makeDefaultOptions();
        service.generateMapping(options);

        // Only the bottom mapping should exist
        assertTrue(Files.exists(project.getAggregateBottomMappingPath()));
        assertFalse(Files.exists(project.getAggregateTopMappingPath()));

        SourceFilesInfo info = service.loadMappings();
        assertMappingPresent(info, "grp:groupa:group1", "group1", aggrPath1);
        assertEquals(1, info.getMappings().size());
    }

    private void setupGroupedIndex() throws Exception {
        var options = new GroupMappingOptions();
        options.setGroupField("groupa");
        testHelper.getGroupMappingService().generateMapping(options);
        var syncOptions = new GroupMappingSyncOptions();
        syncOptions.setSortField("file");
        testHelper.getGroupMappingService().syncMappings(syncOptions);
    }

    private AggregateFileMappingOptions makeDefaultOptions() {
        var options = new AggregateFileMappingOptions();
        options.setBasePath(basePath);
        options.setExportField("groupa");
        options.setFieldMatchingPattern("(.+)");
        options.setFilenameTemplate("$1.pdf");
        return options;
    }

    private void assertExceptionContains(String expected, Exception e) {
        assertTrue(e.getMessage().contains(expected),
                "Expected message exception to contain '" + expected + "', but was: " + e.getMessage());
    }

    private void assertMappingFileNotCreated() {
        assertFalse(Files.exists(project.getAggregateTopMappingPath()), "Mapping file should not exist");
    }

    private void assertMappingPresent(SourceFilesInfo info, String cdmid, String matchingVal, Path... sourcePath) {
        List<SourceFilesInfo.SourceFileMapping> mappings = info.getMappings();
        SourceFilesInfo.SourceFileMapping mapping = mappings.stream().filter(m -> m.getCdmId().equals(cdmid)).findFirst().get();

        if (sourcePath == null) {
            assertNull(mapping.getSourcePaths());
        } else {
            assertEquals(Arrays.asList(sourcePath), mapping.getSourcePaths());
        }
        assertEquals(matchingVal, mapping.getMatchingValue());
        assertNull(mapping.getPotentialMatches());
    }

    private void assertMappingPresentWithPotentials(SourceFilesInfo info, String cdmid, String matchingVal, Path... potentialPaths) {
        List<SourceFilesInfo.SourceFileMapping> mappings = info.getMappings();
        SourceFilesInfo.SourceFileMapping mapping = mappings.stream().filter(m -> m.getCdmId().equals(cdmid)).findFirst().get();

        assertNull(mapping.getSourcePaths());
        assertEquals(matchingVal, mapping.getMatchingValue());
        for (Path potentialPath : potentialPaths) {
            assertTrue(mapping.getPotentialMatches().contains(potentialPath.toString()),
                    "Mapping did not contain expected potential path: " + potentialPath);
        }
    }
}
