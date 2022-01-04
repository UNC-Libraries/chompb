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
package edu.unc.lib.boxc.migration.cdm.services.export;

import edu.unc.lib.boxc.migration.cdm.util.DisplayProgressUtil;
import org.slf4j.Logger;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
    private boolean listingNeedsClose;
    private boolean exportingNeedsClose;

    private Thread displayThread;
    private AtomicBoolean displayActive = new AtomicBoolean(false);
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
        displayActive.set(true);
        displayThread = new Thread(() -> {
            // Update the progress display until flag is set to end it, or an interrupt occurs
            while (displayActive.get()) {
                update();
                try {
                    TimeUnit.MILLISECONDS.sleep(displayUpdateRate);
                } catch (InterruptedException e) {
                    log.warn("Interrupting progress display");
                    displayActive.set(false);
                    DisplayProgressUtil.finishProgress();
                    return;
                } catch (Exception e) {
                    displayActive.set(false);
                    DisplayProgressUtil.finishProgress();
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
        if (displayActive.get()) {
            // Wait up to 10 ticks for the display thread to end, just in case it doesn't end immediately
            try {
                // Wait a tick to allow display to sync up with the state
                TimeUnit.MILLISECONDS.sleep(displayUpdateRate);
                displayActive.set(false);
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
            // First entered counting state
            if (lastUpdateState == null) {
                outputLogger.info("Determining size of collection for export...");
            }
        }
        if (ProgressState.LISTING_OBJECTS.equals(currentProgress)) {
            // Transitioning into listing state
            if (!ProgressState.LISTING_OBJECTS.equals(lastUpdateState)) {
                outputLogger.info("Listing CDM Object IDs:");
                listingNeedsClose = true;
            }
            DisplayProgressUtil.displayProgress(currentState.getListedObjectCount(), currentState.getTotalObjects());
            return;
        } else if (listingNeedsClose) {
            // Final update of progress, to make sure it reaches end
            DisplayProgressUtil.displayProgress(currentState.getListedObjectCount(), currentState.getTotalObjects());
            DisplayProgressUtil.finishProgress();
            listingNeedsClose = false;
        }

        // export count is the index plus one, unless none have been exported yet
        int exportCount = currentState.getLastExportedIndex() == 0 ? 0 : (currentState.getLastExportedIndex() + 1);
        if (ProgressState.EXPORTING.equals(currentProgress)) {
            // Transitioning into listing state
            if (!ProgressState.EXPORTING.equals(lastUpdateState)) {
                outputLogger.info("Exporting object metadata:");
                exportingNeedsClose = true;
            }
            DisplayProgressUtil.displayProgress(exportCount, currentState.getTotalObjects());
            return;
        } else if (exportingNeedsClose) {
            // Final update of progress, to make sure it reaches end
            DisplayProgressUtil.displayProgress(exportCount, currentState.getTotalObjects());
            DisplayProgressUtil.finishProgress();
            exportingNeedsClose = false;
        }
    }

    public void setExportStateService(ExportStateService exportStateService) {
        this.exportStateService = exportStateService;
    }

    public void setDisplayUpdateRate(long displayUpdateRate) {
        this.displayUpdateRate = displayUpdateRate;
    }
}
