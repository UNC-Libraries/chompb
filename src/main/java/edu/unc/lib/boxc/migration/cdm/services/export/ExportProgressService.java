package edu.unc.lib.boxc.migration.cdm.services.export;

import org.slf4j.Logger;

import java.util.concurrent.TimeUnit;

import static edu.unc.lib.boxc.migration.cdm.services.export.ExportState.ProgressState;
import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Service which displays progress of an export operation
 *
 * @author bbpennel
 */
public class ExportProgressService {
    private static final Logger log = getLogger(ExportProgressService.class);

    private ProgressState previousProgressState;
    private ExportStateService exportStateService;

    private Thread displayThread;
    private boolean displayActive;
    private static final long DEFAULT_UPDATE_RATE = 250l;
    private long displayUpdateRate = DEFAULT_UPDATE_RATE;

    /**
     * Start display of progress in a separate thread.
     */
    public void startProgressDisplay() {
        if (displayThread != null) {
            log.error("Progress display already active, cannot start again");
            return;
        }
        displayActive = true;
        displayThread = new Thread(() -> {
            // Update the progress display until flag is set to end it, or an interrupt occurs
            while (displayActive) {
                update();
                try {
                    TimeUnit.MILLISECONDS.sleep(displayUpdateRate);
                } catch (InterruptedException e) {
                    log.warn("Interrupting progress display");
                    displayActive = false;
                    return;
                } catch (Exception e) {
                    displayActive = false;
                    throw e;
                }
            }
        });
        displayThread.start();
    }

    /**
     * End refreshing of the progress display, waiting for the display to terminate
     */
    public void endProgressDisplay() {
        if (displayThread == null) {
            log.error("Progress display not active");
            return;
        }
        if (displayActive) {
            // Wait up to 10 ticks for the display thread to end, just in case it doesn't end immediately
            try {
                // Wait a tick to allow display to sync up with the state
                TimeUnit.MILLISECONDS.sleep(displayUpdateRate);
                displayActive = false;
                // Allow thread time to shut down
                displayThread.join(displayUpdateRate * 9);
            } catch (InterruptedException e) {
                log.warn("Interrupted while waiting for progress display to shut down");
            }
        }
        displayThread = null;
    }

    /**
     * Update the display of progress for the active export
     */
    public void update() {
        ProgressState lastUpdateState = previousProgressState;
        ExportState currentState = exportStateService.getState();
        previousProgressState = currentState.getProgressState();

        ProgressState currentProgress = currentState.getProgressState();
        // Display counting message if starting or we missed the starting state and haven't displayed message yet
        if (ProgressState.STARTING.equals(currentProgress)
                || (currentProgress != null && !currentState.isResuming() && lastUpdateState == null)) {
            if (lastUpdateState == null) {
                outputLogger.info("Initiating export...");
            }
        }
        if (ProgressState.DOWNLOADING_DESC.equals(currentProgress)) {
            // Transitioning into download desc
            if (!ProgressState.DOWNLOADING_DESC.equals(lastUpdateState)) {
                outputLogger.info("Retrieving description file for collection...");
            }
        }
        if (ProgressState.DOWNLOADING_CPD.equals(currentProgress)) {
            // Transitioning into download cpds
            if (!ProgressState.DOWNLOADING_CPD.equals(lastUpdateState)) {
                outputLogger.info("Retrieving compound object files for collection...");
            }
        }
        if (ProgressState.DOWNLOADING_PDF.equals(currentProgress)) {
            // Transitioning into download pdfs
            if (!ProgressState.DOWNLOADING_PDF.equals(lastUpdateState)) {
                outputLogger.info("Retrieving pdf object files for collection...");
            }
        }
        if (ProgressState.EXPORT_COMPLETED.equals(currentProgress)) {
            // Transitioning into completed state
            if (!ProgressState.EXPORT_COMPLETED.equals(lastUpdateState)) {
                outputLogger.info("Finished exporting");
            }
        }
    }

    public void setExportStateService(ExportStateService exportStateService) {
        this.exportStateService = exportStateService;
    }

    public void setDisplayUpdateRate(long displayUpdateRate) {
        this.displayUpdateRate = displayUpdateRate;
    }
}
