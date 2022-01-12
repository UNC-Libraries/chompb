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

import edu.unc.lib.boxc.migration.cdm.AbstractOutputTest;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import org.junit.Before;
import org.junit.Test;

import static edu.unc.lib.boxc.migration.cdm.services.export.ExportState.ProgressState;

/**
 * @author bbpennel
 */
public class ExportProgressServiceTest extends AbstractOutputTest {
    private static final String PROJECT_NAME = "proj";

    private MigrationProject project;
    private ExportStateService exportStateService;
    private ExportProgressService exportProgressService;

    @Before
    public void setup() throws Exception {
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder.getRoot().toPath(), PROJECT_NAME, null, "user");
        exportStateService = new ExportStateService();
        exportStateService.setProject(project);
        exportProgressService = new ExportProgressService();
        exportProgressService.setExportStateService(exportStateService);
    }

    @Test
    public void updateProgressNotStartedTest() throws Exception {
        exportProgressService.update();

        assertOutputDoesNotContain("Determining size of collection for export");
    }

    @Test
    public void updateProgressStartingTest() throws Exception {
        exportStateService.getState().setProgressState(ProgressState.STARTING);

        exportProgressService.update();

        assertOutputContains("Determining size of collection for export");
    }

    @Test
    public void updateListingFromNotStartedTest() throws Exception {
        exportStateService.getState().setProgressState(ProgressState.LISTING_OBJECTS);
        exportStateService.getState().setTotalObjects(160);

        exportProgressService.update();

        assertOutputContains("Determining size of collection for export");
        assertOutputMatches(".*Listing CDM Object IDs:\n.* 0/160.*");
    }

    @Test
    public void updateListingFromCountCompletedTest() throws Exception {
        exportStateService.getState().setProgressState(ProgressState.COUNT_COMPLETED);
        exportStateService.getState().setTotalObjects(160);
        exportProgressService.update();

        resetOutput();

        exportStateService.getState().setListedObjectCount(50);
        exportStateService.getState().setProgressState(ProgressState.LISTING_OBJECTS);

        exportProgressService.update();

        assertOutputDoesNotContain("Determining size of collection for export");
        assertOutputMatches(".*Listing CDM Object IDs:\n.* 50/160.*");
    }

    @Test
    public void updateListingThroughCompleteTest() throws Exception {
        exportStateService.getState().setProgressState(ProgressState.COUNT_COMPLETED);
        exportStateService.getState().setTotalObjects(160);
        exportProgressService.update();
        resetOutput();

        exportStateService.getState().setProgressState(ProgressState.LISTING_OBJECTS);

        exportStateService.getState().incrementListedObjectCount(50);
        exportProgressService.update();
        assertOutputMatches(".*Listing CDM Object IDs:\n.* 50/160.*");
        resetOutput();

        // Update called without any changes
        exportProgressService.update();
        assertOutputMatches(".* 50/160.*");
        resetOutput();

        exportStateService.getState().incrementListedObjectCount(50);
        exportProgressService.update();
        assertOutputMatches(".* 100/160.*");
        resetOutput();

        exportStateService.getState().incrementListedObjectCount(50);
        exportProgressService.update();
        assertOutputMatches(".* 150/160.*");
        resetOutput();

        exportStateService.getState().incrementListedObjectCount(10);
        exportStateService.getState().setProgressState(ProgressState.COUNT_COMPLETED);
        exportProgressService.update();
        assertOutputMatches(".* 160/160.*");
    }

    @Test
    public void updateExportFromListingTest() throws Exception {
        exportStateService.getState().setProgressState(ProgressState.LISTING_OBJECTS);
        exportStateService.getState().setTotalObjects(160);
        exportStateService.getState().setListedObjectCount(160);
        exportProgressService.update();
        resetOutput();

        exportStateService.getState().setProgressState(ProgressState.EXPORTING);
        exportProgressService.update();
        // This is the finalization of the listing progress, since we skipped the completed step
        assertOutputMatches(".* 160/160.*");
        assertOutputMatches(".*Exporting object metadata:\n.* 0/160.*");
    }

    @Test
    public void updateExportFromListCompletedTest() throws Exception {
        exportStateService.getState().setProgressState(ProgressState.LISTING_COMPLETED);
        exportStateService.getState().setTotalObjects(160);
        exportStateService.getState().setListedObjectCount(160);
        exportProgressService.update();
        resetOutput();

        exportStateService.getState().setProgressState(ProgressState.EXPORTING);
        exportProgressService.update();
        assertOutputMatches(".*Exporting object metadata:\n.* 0/160.*");
        resetOutput();

        exportStateService.getState().setLastExportedIndex(99);
        exportProgressService.update();
        assertOutputMatches(".* 100/160.*");
        resetOutput();

        exportStateService.getState().setLastExportedIndex(159);
        exportStateService.getState().setProgressState(ProgressState.EXPORT_COMPLETED);
        exportProgressService.update();
        assertOutputMatches(".* 160/160.*");
    }

    @Test
    public void displayProgressTest() throws Exception {
        try {
            exportProgressService.setDisplayUpdateRate(10);
            exportProgressService.startProgressDisplay();

            assertOutputDoesNotContain("Determining size of collection for export");

            exportStateService.getState().setProgressState(ProgressState.STARTING);
            awaitOutputMatches(".*Determining size of collection for export.*");

            exportStateService.getState().setTotalObjects(160);
            exportStateService.getState().setProgressState(ProgressState.COUNT_COMPLETED);

            exportStateService.getState().setProgressState(ProgressState.LISTING_OBJECTS);
            awaitOutputMatches(".*Listing CDM Object IDs:\n.* 0/160.*");

            exportStateService.getState().setListedObjectCount(100);
            awaitOutputMatches(".* 100/160.*");

            exportStateService.getState().setListedObjectCount(160);
            exportStateService.getState().setProgressState(ProgressState.LISTING_COMPLETED);

            exportStateService.getState().setProgressState(ProgressState.EXPORTING);
            awaitOutputMatches(".*Exporting object metadata:\n.* 0/160.*");

            exportStateService.getState().setLastExportedIndex(159);
            exportStateService.getState().setProgressState(ProgressState.EXPORT_COMPLETED);
            awaitOutputMatches(".* 160/160.*");
        } finally {
            exportProgressService.endProgressDisplay();
        }
    }
}
