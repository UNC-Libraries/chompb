package edu.unc.lib.boxc.migration.cdm.services.export;

import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.CdmFileRetrievalService;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.test.BxcEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static edu.unc.lib.boxc.migration.cdm.services.export.ExportState.ProgressState;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author bbpennel
 */
public class ExportStateServiceTest {
    private static final String PROJECT_NAME = "proj";
    @TempDir
    public Path tmpFolder;

    private ExportStateService exportStateService;
    private MigrationProject project;

    @BeforeEach
    public void setup() throws Exception {
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder, PROJECT_NAME, null, "user", CdmEnvironmentHelper.DEFAULT_ENV_ID,
                BxcEnvironmentHelper.DEFAULT_ENV_ID, MigrationProject.PROJECT_SOURCE_CDM);
        Files.copy(Paths.get("src/test/resources/gilmer_fields.csv"), project.getFieldsPath());
        exportStateService = new ExportStateService();
        exportStateService.setProject(project);
    }

    @Test
    public void startOrResumeExportNewRun() throws Exception {
        exportStateService.startOrResumeExport(false);
        ExportState state = exportStateService.readState();
        assertFalse(state.isResuming());
        assertEquals(ProgressState.STARTING, state.getProgressState());
    }

    @Test
    public void startOrResumeExportNewRunWithForceRestart() throws Exception {
        exportStateService.startOrResumeExport(true);
        ExportState state = exportStateService.readState();
        assertFalse(state.isResuming());
        assertEquals(ProgressState.STARTING, state.getProgressState());
    }

    @Test
    public void startOrResumeExportResumeInStartingState() throws Exception {
        ExportState initState = exportStateService.getState();
        initState.setProgressState(ProgressState.STARTING);
        exportStateService.writeState();

        exportStateService.startOrResumeExport(false);
        ExportState state = exportStateService.readState();
        assertFalse(state.isResuming());
        assertEquals(ProgressState.STARTING, state.getProgressState());
    }

    @Test
    public void startOrResumeExportResumeInCompletedState() throws Exception {
        exportStateService.startOrResumeExport(false);
        exportStateService.transitionToDownloadingDesc();
        exportStateService.transitionToDownloadingCpd();
        exportStateService.transitionToDownloadingPdf();
        exportStateService.exportingCompleted();

        try {
            exportStateService.startOrResumeExport(false);
            fail();
        } catch (InvalidProjectStateException e) {
            assertTrue(e.getMessage().contains("Export has already completed"));
        }
        ExportState state = exportStateService.readState();
        assertFalse(state.isResuming());
        assertEquals(ProgressState.EXPORT_COMPLETED, state.getProgressState());
    }

    @Test
    public void startOrResumeExportResumeInCompletedStateWithRestart() throws Exception {
        exportStateService.startOrResumeExport(false);
        exportStateService.transitionToDownloadingDesc();
        exportStateService.transitionToDownloadingCpd();
        exportStateService.transitionToDownloadingPdf();
        exportStateService.exportingCompleted();

        exportStateService.startOrResumeExport(true);

        ExportState state = exportStateService.readState();
        assertFalse(state.isResuming());
        assertEquals(ProgressState.STARTING, state.getProgressState());
    }

    @Test
    public void startOrResumeExportResumeInDownloadingCpdState() throws Exception {
        exportStateService.startOrResumeExport(false);
        exportStateService.transitionToDownloadingDesc();
        exportStateService.transitionToDownloadingCpd();

        var cpdPath = CdmFileRetrievalService.getExportedCpdsPath(project);
        Files.createDirectories(cpdPath);

        exportStateService.startOrResumeExport(false);

        // Persisted state should not be modified during resumption
        ExportState state = exportStateService.readState();
        assertFalse(state.isResuming());
        assertEquals(ProgressState.DOWNLOADING_CPD, state.getProgressState());
        assertTrue(Files.isDirectory(cpdPath), "CPD export path must still exist");
    }

    @Test
    public void startOrResumeExportResumeInExportingStateWithForceRestart() throws Exception {
        exportStateService.startOrResumeExport(false);
        exportStateService.transitionToDownloadingDesc();
        exportStateService.transitionToDownloadingCpd();

        var cpdPath = CdmFileRetrievalService.getExportedCpdsPath(project);
        Files.createDirectories(cpdPath);

        exportStateService.startOrResumeExport(true);

        ExportState state = exportStateService.getState();
        assertFalse(state.isResuming());
        assertEquals(ProgressState.STARTING, state.getProgressState());
        assertFalse(Files.isDirectory(cpdPath), "CPD export path must not exist");
    }

    @Test
    public void inStateOrNotResumingNoResumeTest() throws Exception {
        assertTrue(exportStateService.inStateOrNotResuming());
    }

    @Test
    public void inStateOrNotResumingNoResumeInStateTest() throws Exception {
        ExportState state = exportStateService.getState();
        state.setProgressState(ProgressState.DOWNLOADING_DESC);
        assertTrue(exportStateService.inStateOrNotResuming(ProgressState.DOWNLOADING_DESC));
    }

    @Test
    public void inStateOrNotResumingIsResumingInStateTest() throws Exception {
        ExportState state = exportStateService.getState();
        state.setResuming(true);
        state.setProgressState(ProgressState.DOWNLOADING_CPD);
        assertTrue(exportStateService.inStateOrNotResuming(ProgressState.DOWNLOADING_CPD));
        assertTrue(exportStateService.inStateOrNotResuming(ProgressState.DOWNLOADING_CPD,
                ProgressState.DOWNLOADING_DESC));
    }

    @Test
    public void inStateOrNotResumingIsResumingNotInStateTest() throws Exception {
        ExportState state = exportStateService.getState();
        state.setResuming(true);
        state.setProgressState(ProgressState.DOWNLOADING_DESC);
        assertFalse(exportStateService.inStateOrNotResuming(ProgressState.DOWNLOADING_CPD));
        assertFalse(exportStateService.inStateOrNotResuming(ProgressState.STARTING,
                ProgressState.DOWNLOADING_CPD));
    }

    @Test
    public void transitionToDownloadingTest() throws Exception {
        exportStateService.startOrResumeExport(false);
        exportStateService.getState().setProgressState(ProgressState.STARTING);
        exportStateService.writeState();

        exportStateService.transitionToDownloadingDesc();

        ExportState state = exportStateService.readState();
        assertEquals(ProgressState.DOWNLOADING_DESC, state.getProgressState());
    }

    @Test
    public void transitionToDownloadingInWrongStateTest() throws Exception {
        exportStateService.startOrResumeExport(false);
        exportStateService.getState().setProgressState(ProgressState.DOWNLOADING_CPD);
        exportStateService.writeState();

        try {
            exportStateService.transitionToDownloadingDesc();
            fail();
        } catch (InvalidProjectStateException e) {
        }
        ExportState state = exportStateService.readState();
        assertEquals(ProgressState.DOWNLOADING_CPD, state.getProgressState());
    }

    @Test
    public void transitionToDownloadingCpdTest() throws Exception {
        exportStateService.startOrResumeExport(false);
        exportStateService.getState().setProgressState(ProgressState.DOWNLOADING_DESC);
        exportStateService.writeState();

        exportStateService.transitionToDownloadingCpd();

        ExportState state = exportStateService.readState();
        assertEquals(ProgressState.DOWNLOADING_CPD, state.getProgressState());
    }

    @Test
    public void transitionToDownloadingCpdWrongStateTest() throws Exception {
        exportStateService.startOrResumeExport(false);

        try {
            exportStateService.transitionToDownloadingCpd();
            fail();
        } catch (InvalidProjectStateException e) {
        }
        ExportState state = exportStateService.readState();
        assertEquals(ProgressState.STARTING, state.getProgressState());
    }

    @Test
    public void exportingCompletedTest() throws Exception {
        exportStateService.startOrResumeExport(false);
        exportStateService.getState().setProgressState(ProgressState.DOWNLOADING_PDF);
        exportStateService.exportingCompleted();

        ExportState state = exportStateService.readState();
        assertEquals(ProgressState.EXPORT_COMPLETED, state.getProgressState());
    }

    private List<String> generateIdList(int count) {
        return IntStream.range(0, count)
                .mapToObj(Integer::toString)
                .collect(Collectors.toList());
    }
}
