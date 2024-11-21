package edu.unc.lib.boxc.migration.cdm.services.export;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

import static edu.unc.lib.boxc.migration.cdm.services.export.ExportState.ProgressState;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Service which records and tracks the state of an export operation
 *
 * @author bbpennel
 */
public class ExportStateService {
    private static final Logger log = getLogger(ExportStateService.class);
    private static final String STATE_FILENAME = ".export_state.json";
    private static final ObjectWriter STATE_WRITER;
    private static final ObjectReader STATE_READER;
    static {
        JavaTimeModule module = new JavaTimeModule();
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(module);
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

        STATE_READER = mapper.readerFor(ExportState.class);
        STATE_WRITER = mapper.writerFor(ExportState.class);
    }

    private MigrationProject project;
    private ExportState state = new ExportState();

    /**
     * Initializes the state of the export to either a new export or resumes an incomplete export when
     * appropriate. If the forceRestart flag is provided, or the previous incomplete export did
     * not progress far enough to warrant resumption, the existing state will be clear and restarted.
     *
     * @param forceRestart if true, previous export state will be cleared and a new export state started
     * @throws IOException
     */
    public void startOrResumeExport(boolean forceRestart) throws IOException {
        if (forceRestart) {
            clearState();
            transitionToStarting();
            log.debug("Forcing restart of export, cleared previous state");
            return;
        }
        state = readState();
        ProgressState progressState = state.getProgressState();
        if (progressState == null) {
            transitionToStarting();
            log.debug("Starting new export");
            return;
        }
        // If resuming in initial steps, start over
        if (ProgressState.STARTING.equals(progressState) || ProgressState.DOWNLOADING_DESC.equals(progressState)) {
            clearState();
            transitionToStarting();
            log.debug("Restarting export, had not reached listing or exporting states");
            return;
        }
        if (ProgressState.EXPORT_COMPLETED.equals(progressState)) {
            throw new InvalidProjectStateException("Export has already completed, must force restart to overwrite");
        }
        // For all other states, we are resuming
        state.setResuming(true);
    }

    /**
     * Check if the export is either not resuming or is in one of the listed states. Used to determine if
     * the steps for a stage of an export should be executed or not.
     * @param expectedStates
     * @return
     */
    public boolean inStateOrNotResuming(ProgressState... expectedStates) {
        if (!state.isResuming()) {
            return true;
        }
        return ExportState.inState(state, expectedStates);
    }

    public boolean isResuming() {
        return state.isResuming();
    }

    /**
     * Transition the export to the listing object ids state
     * @throws IOException
     */
    public void transitionToStarting() throws IOException {
        state.setProgressState(ProgressState.STARTING);
        state.setStartTime(Instant.now());
        writeState();
    }

    /**
     * Transition to downloading desc file
     * @throws IOException
     */
    public void transitionToDownloadingDesc() throws IOException {
        assertState(ProgressState.STARTING);
        state.setProgressState(ProgressState.DOWNLOADING_DESC);
        writeState();
    }

    /**
     * Transition to downloading CPD files
     * @throws IOException
     */
    public void transitionToDownloadingCpd() throws IOException {
        assertState(ProgressState.DOWNLOADING_DESC);
        state.setProgressState(ProgressState.DOWNLOADING_CPD);
        writeState();
    }

    /**
     * Transition to downloading PDF files
     * @throws IOException
     */
    public void transitionToDownloadingPdf() throws IOException {
        assertState(ProgressState.DOWNLOADING_CPD);
        state.setProgressState(ProgressState.DOWNLOADING_PDF);
        writeState();
    }

    /**
     * Indicate that the export step has completed
     * @throws IOException
     */
    public void exportingCompleted() throws IOException {
        assertState(ProgressState.DOWNLOADING_PDF);
        state.setProgressState(ProgressState.EXPORT_COMPLETED);
        writeState();
    }

    public Path getExportStatePath() {
        return project.getExportPath().resolve(STATE_FILENAME);
    }

    /**
     * Throws an InvalidProjectStateException if the state of the export does not match the expected state
     * @param expectedState
     */
    public void assertState(ProgressState expectedState) {
        if (!state.getProgressState().equals(expectedState)) {
            throw new InvalidProjectStateException("Invalid state, export must be in " + expectedState + " state"
                + " but was in state " + state.getProgressState());
        }
    }

    /**
     * Serialize the state of the export operation
     * @throws IOException
     */
    public void writeState() throws IOException {
        if (Files.notExists(project.getExportPath())) {
            Files.createDirectories(project.getExportPath());
        }
        STATE_WRITER.writeValue(getExportStatePath().toFile(), state);
    }

    /**
     * @return Deserialized state of the export operation
     * @throws IOException
     */
    public ExportState readState() throws IOException {
        if (Files.notExists(getExportStatePath())) {
            return new ExportState();
        }
        return STATE_READER.readValue(getExportStatePath().toFile());
    }

    /**
     * Clear any existing export state for the project
     * @throws IOException
     */
    public void clearState() throws IOException {
        if (Files.exists(project.getExportPath())) {
            FileUtils.deleteDirectory(project.getExportPath().toFile());
        }
        state = new ExportState();
    }

    public void setProject(MigrationProject project) {
        this.project = project;
    }

    public ExportState getState() {
        return state;
    }
}
