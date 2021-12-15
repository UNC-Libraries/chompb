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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static edu.unc.lib.boxc.migration.cdm.services.export.ExportState.ProgressState;

/**
 * @author bbpennel
 */
public class ExportStateService {
    private static final String IDS_FILENAME = ".object_ids.txt";
    private static final String STATE_FILENAME = ".export_state.json";
    private MigrationProject project;
    private ExportState state;
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

    /**
     * Transition the export to the listing object ids state
     * @throws IOException
     */
    public void transitionToStarting() throws IOException {
        state = new ExportState();
        state.setState(ProgressState.STARTING);
        writeState();
    }

    /**
     * Record the count of objects in this collection has completed
     * @param count
     * @throws IOException
     */
    public void objectCountCompleted(int count) throws IOException {
        assertState(ProgressState.STARTING);
        state.setTotalObjects(count);
        state.setState(ExportState.ProgressState.COUNT_COMPLETED);
        writeState();
    }

    /**
     * Transition the export to the listing object ids state
     * @param listingPageSize
     * @throws IOException
     */
    public void transitionToListing(int listingPageSize) throws IOException {
        assertState(ProgressState.COUNT_COMPLETED);
        state.setState(ProgressState.LISTING_OBJECTS);
        state.setListIdPageSize(listingPageSize);
        writeState();
    }

    /**
     * Indicate that listing of object ids has completed
     * @throws IOException
     */
    public void listingComplete() throws IOException {
        assertState(ProgressState.LISTING_OBJECTS);
        state.setState(ProgressState.LISTING_COMPLETED);
        writeState();
    }

    /**
     * Record a list of object ids which are included in this export
     * @param ids
     */
    public void registerObjectIds(List<String> ids) throws IOException {
        assertState(ProgressState.LISTING_OBJECTS);
        String idsJoined = String.join("\r\n", ids);
        Files.write(
                getObjectListPath(),
                idsJoined.getBytes(),
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    /**
     * @return the list of object ids for this CDM project
     */
    public List<String> retrieveObjectIds() throws IOException {
        try (Stream<String> lines = Files.lines(getObjectListPath())) {
            return lines.collect(Collectors.toList());
        }
    }

    /**
     * Transition the export to the exporting objects state
     * @param exportPageSize
     * @throws IOException
     */
    public void transitionToExporting(int exportPageSize) throws IOException {
        assertState(ProgressState.LISTING_COMPLETED);
        state.setState(ProgressState.EXPORTING);
        state.setExportPageSize(exportPageSize);
        writeState();
    }

    /**
     * Register a list of object ids as having been exported
     * @param objectIds
     * @throws IOException
     */
    public void registerExported(List<String> objectIds) throws IOException {
        assertState(ProgressState.EXPORTING);
        int lastIndex = state.getLastExportedIndex() == null ? 0 : state.getLastExportedIndex();
        state.setLastExportedIndex(lastIndex + objectIds.size() - 1);
        writeState();
    }

    /**
     * Indicate that the export step has completed
     * @throws IOException
     */
    public void exportingCompleted() throws IOException {
        assertState(ProgressState.EXPORTING);
        state.setState(ProgressState.EXPORT_COMPLETED);
        writeState();
    }

    public Path getObjectListPath() {
        return project.getExportPath().resolve(IDS_FILENAME);
    }

    public Path getExportStatePath() {
        return project.getExportPath().resolve(STATE_FILENAME);
    }

    private void assertState(ProgressState expectedState) {
        if (!state.getState().equals(expectedState)) {
            throw new InvalidProjectStateException("Invalid state, export must be in " + expectedState + " state");
        }
    }

    /**
     * Serialize the state of the export operation
     * @throws IOException
     */
    public void writeState() throws IOException {
        STATE_WRITER.writeValue(getExportStatePath().toFile(), state);
    }

    /**
     * @return Deserialized state of the export operation
     * @throws IOException
     */
    public ExportState readState() throws IOException {
        return STATE_READER.readValue(getExportStatePath().toFile());
    }

    /**
     * Clear any existing export state for the project
     * @throws IOException
     */
    public void clearState() throws IOException {
        Files.deleteIfExists(getExportStatePath());
        Files.deleteIfExists(getObjectListPath());
        state = new ExportState();
    }

    public void setProject(MigrationProject project) {
        this.project = project;
    }
}
