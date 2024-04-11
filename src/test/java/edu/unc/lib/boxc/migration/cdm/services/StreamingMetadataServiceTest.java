package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.test.BxcEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.SipServiceHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.MockitoAnnotations.openMocks;

public class StreamingMetadataServiceTest {
    private static final String PROJECT_NAME = "proj";

    @TempDir
    public Path tmpFolder;

    private SipServiceHelper testHelper;
    private MigrationProject project;
    private StreamingMetadataService service;
    private AutoCloseable closeable;

    @BeforeEach
    public void setup() throws Exception {
        closeable = openMocks(this);
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder, PROJECT_NAME, null, "user",
                CdmEnvironmentHelper.DEFAULT_ENV_ID, BxcEnvironmentHelper.DEFAULT_ENV_ID);
        testHelper = new SipServiceHelper(project, tmpFolder);
        service = new StreamingMetadataService();
        service.setProject(project);
        service.setFieldService(testHelper.getFieldService());
        service.setIndexService(testHelper.getIndexService());
    }

    @AfterEach
    void closeService() throws Exception {
        closeable.close();
    }

    @Test
    public void verifyNoStreamingMetadata() throws Exception {
        testHelper.indexExportData("mini_gilmer");

        var result = service.verifyRecordHasStreamingMetadata("25");
        assertFalse(result);
    }

    @Test
    public void verifyHasStreamingMetadata() throws Exception {
        testHelper.indexExportData("mini_gilmer");

        var result = service.verifyRecordHasStreamingMetadata("27");
        assertTrue(result);
    }

    @Test
    public void getStreamingMetadataSuccess() throws Exception {
        testHelper.indexExportData("mini_gilmer");

        var result = service.getStreamingMetadata("27");
        assertEquals("gilmer_recording-playlist.m3u8", result[0]);
        assertEquals("open-hls", result[1]);
        assertEquals("duracloud", result[2]);
    }

    @Test
    public void getStreamingMetadataFail() throws Exception {
        testHelper.indexExportData("mini_gilmer");

        Exception exception = assertThrows(NullPointerException.class, () -> {
            service.getStreamingMetadata("25");
        });
    }
}
