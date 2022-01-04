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

import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static edu.unc.lib.boxc.migration.cdm.services.export.ExportState.ProgressState;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author bbpennel
 */
public class ExportStateServiceTest {
    private static final String PROJECT_NAME = "proj";
    @Rule
    public final TemporaryFolder tmpFolder = new TemporaryFolder();

    private ExportStateService exportStateService;
    private MigrationProject project;

    @Before
    public void setup() throws Exception {
        tmpFolder.create();
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder.getRoot().toPath(), PROJECT_NAME, null, "user");
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
    public void startOrResumeExportResumeInCountedState() throws Exception {
        ExportState initState = exportStateService.getState();
        initState.setProgressState(ProgressState.COUNT_COMPLETED);
        initState.setTotalObjects(99);
        exportStateService.writeState();

        exportStateService.startOrResumeExport(false);
        ExportState state = exportStateService.readState();
        assertFalse(state.isResuming());
        assertEquals(ProgressState.STARTING, state.getProgressState());
        assertNull(state.getTotalObjects());
    }

    @Test
    public void startOrResumeExportResumeInCompletedState() throws Exception {
        exportStateService.startOrResumeExport(false);
        ExportState initState = exportStateService.getState();
        initState.setTotalObjects(99);
        List<String> cdmIds = generateIdList(99);
        initState.setProgressState(ProgressState.LISTING_OBJECTS);
        exportStateService.registerObjectIds(cdmIds);
        initState.setProgressState(ProgressState.EXPORTING);
        exportStateService.registerExported(cdmIds);
        initState.setProgressState(ProgressState.EXPORT_COMPLETED);
        exportStateService.writeState();

        try {
            exportStateService.startOrResumeExport(false);
            fail();
        } catch (InvalidProjectStateException e) {
            assertTrue(e.getMessage().contains("Export has already completed"));
        }
        ExportState state = exportStateService.readState();
        assertFalse(state.isResuming());
        assertEquals(ProgressState.EXPORT_COMPLETED, state.getProgressState());
        assertEquals(99, state.getTotalObjects().intValue());
        assertEquals(cdmIds, exportStateService.retrieveObjectIds());
        assertEquals(cdmIds.size() - 1, state.getLastExportedIndex());
    }

    @Test
    public void startOrResumeExportResumeInCompletedStateWithRestart() throws Exception {
        exportStateService.startOrResumeExport(false);
        ExportState initState = exportStateService.getState();
        initState.setTotalObjects(99);
        List<String> cdmIds = generateIdList(99);
        initState.setProgressState(ProgressState.LISTING_OBJECTS);
        exportStateService.registerObjectIds(cdmIds);
        initState.setProgressState(ProgressState.EXPORTING);
        exportStateService.registerExported(cdmIds);
        initState.setProgressState(ProgressState.EXPORT_COMPLETED);
        exportStateService.writeState();

        exportStateService.startOrResumeExport(true);

        ExportState state = exportStateService.readState();
        assertFalse(state.isResuming());
        assertEquals(ProgressState.STARTING, state.getProgressState());
        assertNull(state.getTotalObjects());
        assertTrue(exportStateService.retrieveObjectIds().isEmpty());
        assertEquals(0, state.getLastExportedIndex());
    }

    @Test
    public void startOrResumeExportResumeInExportingState() throws Exception {
        exportStateService.startOrResumeExport(false);
        ExportState initState = exportStateService.getState();
        initState.setTotalObjects(99);
        initState.setExportPageSize(50);
        List<String> cdmIds = generateIdList(99);
        initState.setProgressState(ProgressState.LISTING_OBJECTS);
        exportStateService.registerObjectIds(cdmIds);
        initState.setProgressState(ProgressState.EXPORTING);
        exportStateService.registerExported(cdmIds.subList(0, 50));
        exportStateService.writeState();

        exportStateService.startOrResumeExport(false);

        // Persisted state should not be modified during resumption
        ExportState state = exportStateService.readState();
        assertFalse(state.isResuming());
        assertEquals(ProgressState.EXPORTING, state.getProgressState());
        assertEquals(99, state.getTotalObjects().intValue());
        assertEquals(cdmIds, exportStateService.retrieveObjectIds());
        assertEquals(49, state.getLastExportedIndex());

        // Active state should indicate it was resumed
        ExportState activeState = exportStateService.getState();
        assertTrue(activeState.isResuming());
        assertEquals(ProgressState.EXPORTING, activeState.getProgressState());
        assertEquals(49, state.getLastExportedIndex());
    }

    @Test
    public void startOrResumeExportResumeInExportingStateWithForceRestart() throws Exception {
        exportStateService.startOrResumeExport(false);
        ExportState initState = exportStateService.getState();
        initState.setTotalObjects(99);
        initState.setExportPageSize(50);
        List<String> cdmIds = generateIdList(99);
        initState.setProgressState(ProgressState.LISTING_OBJECTS);
        exportStateService.registerObjectIds(cdmIds);
        initState.setProgressState(ProgressState.EXPORTING);
        exportStateService.registerExported(cdmIds.subList(0, 50));
        exportStateService.writeState();

        exportStateService.startOrResumeExport(true);

        ExportState state = exportStateService.getState();
        assertFalse(state.isResuming());
        assertEquals(ProgressState.STARTING, state.getProgressState());
        assertNull(state.getTotalObjects());
        assertTrue(exportStateService.retrieveObjectIds().isEmpty());
        assertEquals(0, state.getLastExportedIndex());
    }

    @Test
    public void inStateOrNotResumingNoResumeTest() throws Exception {
        assertTrue(exportStateService.inStateOrNotResuming());
    }

    @Test
    public void inStateOrNotResumingNoResumeInStateTest() throws Exception {
        ExportState state = exportStateService.getState();
        state.setProgressState(ProgressState.LISTING_OBJECTS);
        assertTrue(exportStateService.inStateOrNotResuming(ProgressState.LISTING_OBJECTS));
    }

    @Test
    public void inStateOrNotResumingIsResumingInStateTest() throws Exception {
        ExportState state = exportStateService.getState();
        state.setResuming(true);
        state.setProgressState(ProgressState.LISTING_OBJECTS);
        assertTrue(exportStateService.inStateOrNotResuming(ProgressState.LISTING_OBJECTS));
        assertTrue(exportStateService.inStateOrNotResuming(ProgressState.LISTING_OBJECTS,
                ProgressState.LISTING_COMPLETED));
    }

    @Test
    public void inStateOrNotResumingIsResumingNotInStateTest() throws Exception {
        ExportState state = exportStateService.getState();
        state.setResuming(true);
        state.setProgressState(ProgressState.EXPORTING);
        assertFalse(exportStateService.inStateOrNotResuming(ProgressState.LISTING_OBJECTS));
        assertFalse(exportStateService.inStateOrNotResuming(ProgressState.LISTING_OBJECTS,
                ProgressState.LISTING_COMPLETED));
    }

    @Test
    public void objectCountCompletedTest() throws Exception {
        exportStateService.startOrResumeExport(false);

        exportStateService.objectCountCompleted(120);

        ExportState state = exportStateService.readState();
        assertEquals(ProgressState.COUNT_COMPLETED, state.getProgressState());
        assertEquals(120, state.getTotalObjects().intValue());
    }

    @Test
    public void objectCountCompletedWrongStateTest() throws Exception {
        exportStateService.startOrResumeExport(false);
        exportStateService.getState().setProgressState(ProgressState.EXPORTING);

        try {
            exportStateService.objectCountCompleted(120);
            fail();
        } catch (InvalidProjectStateException e) {
        }
        ExportState state = exportStateService.readState();
        assertEquals(ProgressState.STARTING, state.getProgressState());
        assertNull(state.getTotalObjects());
    }

    @Test
    public void transitionToListingTest() throws Exception {
        exportStateService.startOrResumeExport(false);
        exportStateService.getState().setProgressState(ProgressState.COUNT_COMPLETED);

        exportStateService.transitionToListing(100);

        ExportState state = exportStateService.readState();
        assertEquals(ProgressState.LISTING_OBJECTS, state.getProgressState());
        assertEquals(100, state.getListIdPageSize().intValue());
    }

    @Test
    public void transitionToListingWrongStateTest() throws Exception {
        exportStateService.startOrResumeExport(false);

        try {
            exportStateService.transitionToListing(100);
            fail();
        } catch (InvalidProjectStateException e) {
        }
        ExportState state = exportStateService.readState();
        assertEquals(ProgressState.STARTING, state.getProgressState());
        assertNull(state.getListIdPageSize());
    }

    @Test
    public void listingCompleteTest() throws Exception {
        exportStateService.startOrResumeExport(false);
        exportStateService.getState().setProgressState(ProgressState.LISTING_OBJECTS);

        exportStateService.listingComplete();

        ExportState state = exportStateService.readState();
        assertEquals(ProgressState.LISTING_COMPLETED, state.getProgressState());
    }

    @Test
    public void listingCompleteWrongStateTest() throws Exception {
        exportStateService.startOrResumeExport(false);

        try {
            exportStateService.listingComplete();
            fail();
        } catch (InvalidProjectStateException e) {
        }
        ExportState state = exportStateService.readState();
        assertEquals(ProgressState.STARTING, state.getProgressState());
    }

    @Test
    public void transitionToExportingTest() throws Exception {
        exportStateService.startOrResumeExport(false);
        exportStateService.getState().setProgressState(ProgressState.LISTING_COMPLETED);

        exportStateService.transitionToExporting(500);

        ExportState state = exportStateService.readState();
        assertEquals(ProgressState.EXPORTING, state.getProgressState());
        assertEquals(500, state.getExportPageSize().intValue());
    }

    @Test
    public void exportingCompletedTest() throws Exception {
        exportStateService.startOrResumeExport(false);
        exportStateService.getState().setProgressState(ProgressState.EXPORTING);

        exportStateService.exportingCompleted();

        ExportState state = exportStateService.readState();
        assertEquals(ProgressState.EXPORT_COMPLETED, state.getProgressState());
    }

    @Test
    public void registerObjectIdsTest() throws Exception {
        exportStateService.startOrResumeExport(false);
        exportStateService.getState().setProgressState(ProgressState.LISTING_OBJECTS);
        assertEquals(0, exportStateService.getState().getListedObjectCount());

        List<String> allIds = generateIdList(100);
        List<String> firstPage = allIds.subList(0, 50);
        exportStateService.registerObjectIds(firstPage);
        assertEquals(firstPage, exportStateService.retrieveObjectIds());
        assertEquals(50, exportStateService.getState().getListedObjectCount());

        List<String> secondPage = allIds.subList(50, 100);
        exportStateService.registerObjectIds(secondPage);
        assertEquals(allIds, exportStateService.retrieveObjectIds());
        assertEquals(100, exportStateService.getState().getListedObjectCount());
    }

    @Test
    public void registerExportedTest() throws Exception {
        exportStateService.startOrResumeExport(false);
        exportStateService.getState().setProgressState(ProgressState.EXPORTING);

        List<String> allIds = generateIdList(120);
        List<String> firstPage = allIds.subList(0, 50);
        List<String> secondPage = allIds.subList(50, 100);
        List<String> thirdPage = allIds.subList(100, 120);

        exportStateService.registerExported(firstPage);
        assertEquals(49, exportStateService.getState().getLastExportedIndex());
        exportStateService.registerExported(secondPage);
        assertEquals(99, exportStateService.getState().getLastExportedIndex());
        exportStateService.registerExported(thirdPage);
        assertEquals(119, exportStateService.getState().getLastExportedIndex());
    }

    private List<String> generateIdList(int count) {
        return IntStream.range(0, count)
                .mapToObj(Integer::toString)
                .collect(Collectors.toList());
    }
}
