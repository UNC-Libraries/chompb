package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProjectProperties;
import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo;
import edu.unc.lib.boxc.migration.cdm.options.AltTextFileMappingOptions;
import edu.unc.lib.boxc.migration.cdm.test.BxcEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.SipServiceHelper;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import org.apache.solr.client.solrj.io.eval.GTestDataSetEvaluator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.MockitoAnnotations.openMocks;

public class AltTextFileServiceTest {
    private static final String PROJECT_NAME = "proj";

    @TempDir
    public Path tmpFolder;
    private Path basePath;
    private MigrationProject project;
    private SipServiceHelper testHelper;
    private AltTextFileService service;

    private AutoCloseable closeable;

    @BeforeEach
    public void setup() throws Exception {
        closeable = openMocks(this);
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder, PROJECT_NAME, null, "user", CdmEnvironmentHelper.DEFAULT_ENV_ID,
                BxcEnvironmentHelper.DEFAULT_ENV_ID, MigrationProject.PROJECT_SOURCE_CDM);
        Files.createDirectories(project.getExportPath());

        basePath = tmpFolder.resolve("testFolder");
        Files.createDirectory(basePath);
        testHelper = new SipServiceHelper(project, basePath);
        service = testHelper.getAltTextFilesService();
        service.setProject(project);
        service.setIndexService(testHelper.getIndexService());
    }

    @AfterEach
    void closeService() throws Exception {
        closeable.close();
    }

    @Test
    public void generateAltTextMappingTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        AltTextFileMappingOptions options = makeDefaultOptions();

        service.generateAltTextMapping(options);

        SourceFilesInfo info = service.loadMappings();
        assertMappingPresent(info, "25", "", filesystemAltTextFile("25_alt_text.txt"));
        assertMappingPresent(info, "26", "", filesystemAltTextFile("26_alttext.txt"));
        assertEquals(2, info.getMappings().size());

        assertUpdatedDatePresent();
    }

    @Test
    public void generateAltTextMappingDryRunTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        AltTextFileMappingOptions options = makeDefaultOptions();
        options.setDryRun(true);

        service.generateAltTextMapping(options);

        SourceFilesInfo info = service.loadMappings();
        assertEquals(0, info.getMappings().size());

        assertUpdatedDateNotPresent();
    }

    @Test
    public void updateAltTextMappingTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        AltTextFileMappingOptions options = makeDefaultOptions();

        service.generateAltTextMapping(options);

        // Add more exported objects
        testHelper.indexExportData("gilmer");

        AltTextFileMappingOptions options2 = makeDefaultOptions();
        options2.setUpdate(true);
        service.generateAltTextMapping(options2);

        SourceFilesInfo info2 = service.loadMappings();
        assertMappingPresent(info2, "25", "", filesystemAltTextFile("25_alt_text.txt"));
        assertMappingPresent(info2, "26", "", filesystemAltTextFile("26_alttext.txt"));
        assertMappingPresent(info2, "28", "", filesystemAltTextFile("28_alt_text.txt"));
        assertEquals(3, info2.getMappings().size());

        assertUpdatedDatePresent();
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

    private AltTextFileMappingOptions makeDefaultOptions() {
        AltTextFileMappingOptions options = new AltTextFileMappingOptions();
        options.setBasePath(Path.of("src/test/resources/alt_text"));
        options.setExtensions(Collections.singletonList("txt"));

        return options;
    }

    private void assertUpdatedDatePresent() throws Exception {
        MigrationProjectProperties props = ProjectPropertiesSerialization.read(project.getProjectPropertiesPath());
        assertNotNull(props.getAltTextFilesUpdatedDate(), "Updated timestamp must be set");
        assertNull(props.getSourceFilesUpdatedDate(), "Source mapping timestamp must not be set");
    }

    private void assertUpdatedDateNotPresent() throws Exception {
        MigrationProjectProperties props = ProjectPropertiesSerialization.read(project.getProjectPropertiesPath());
        assertNull(props.getAltTextFilesUpdatedDate(), "Updated timestamp must not be set");
        assertNull(props.getSourceFilesUpdatedDate(), "Source mapping timestamp must not be set");
    }

    private Path filesystemAltTextFile(String relPath) {
        Path basePath = Path.of("src/test/resources/alt_text");
        return basePath.resolve(relPath);
    }
}
