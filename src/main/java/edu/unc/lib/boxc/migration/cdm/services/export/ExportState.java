package edu.unc.lib.boxc.migration.cdm.services.export;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.time.Instant;
import java.util.Arrays;

/**
 * State of a CDM export operation
 *
 * @author bbpennel
 */
public class ExportState {
    private Instant startTime;
    private boolean resuming = false;
    private ProgressState progressState;

    public enum ProgressState {
        STARTING,
        DOWNLOADING_DESC,
        DOWNLOADING_CPD,
        EXPORT_COMPLETED;
    }

    public Instant getStartTime() {
        return startTime;
    }

    public void setStartTime(Instant startTime) {
        this.startTime = startTime;
    }

    public ProgressState getProgressState() {
        return progressState;
    }

    public void setProgressState(ProgressState progressState) {
        this.progressState = progressState;
    }

    /**
     *
     * @param exportState
     * @param expectedStates
     * @return True if the progressState of the provided exportState matches any of the expected states
     */
    public static boolean inState(ExportState exportState, ProgressState... expectedStates) {
        if (exportState == null) {
            return false;
        }
        ProgressState pState = exportState.getProgressState();
        if (pState == null) {
            return false;
        }
        return Arrays.stream(expectedStates).anyMatch(s -> s.equals(pState));
    }

    @JsonIgnore
    public boolean isResuming() {
        return resuming;
    }

    public void setResuming(boolean resuming) {
        this.resuming = resuming;
    }
}
