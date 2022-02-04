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
package edu.unc.lib.boxc.migration.cdm.services;

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.MigrationSip;
import edu.unc.lib.boxc.model.api.ids.PID;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Service for generating a redirect mapping csv with the following information:
 * CDM Collection ID, CDM Object ID, Box-c Work ID, Box-c File ID
 *
 * @author snluong
 */
public class RedirectMappingService {
    private MigrationProject project;
    private String cdmCollectionId;
    private CSVPrinter csvPrinter;
    public static final String[] CSV_HEADERS = new String[] {
            "cdm_collection_id", "cdm_object_id", "boxc_object_id", "boxc_file_id" };

    public RedirectMappingService(MigrationProject project) {
        this.project = project;
    }

    /**
     * Opens RedirectMapping CSV and sets its path
     */
    public void init() {
        try {
            cdmCollectionId = project.getProjectProperties().getCdmCollectionId();
            BufferedWriter writer = Files.newBufferedWriter(project.getRedirectMappingPath());
            csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(CSV_HEADERS));
        } catch (IOException e) {
            throw new MigrationException("Error creating redirect mapping CSV", e);
        }

    }

    /**
     * Closes RedirectMapping CSV
     */
    public void closeCsv() {
        try {
            csvPrinter.close();
        } catch (IOException e) {
            throw new MigrationException("Error closing redirect mapping CSV", e);
        }
    }

    /**
     * Adds row to Redirect Mapping CSV in the correct order
     * @param cdmObjectId
     * @param boxcObjectId
     * @param boxcFileId
     */
    public void addRow(String cdmObjectId, String boxcObjectId, String boxcFileId) {
        try {
            csvPrinter.printRecord(cdmCollectionId, cdmObjectId, boxcObjectId, boxcFileId);
        } catch (IOException e) {
            throw new MigrationException("Error adding row to redirect mapping CSV", e);
        }
    }

    /**
     * Analyzes list of sips to add collection-specific row(s) to Redirect Mapping CSV
     * @param sips list of MigrationSips
     */
    public void addCollectionRow(List<MigrationSip> sips) throws IOException {
        List<PID> destinationPids = new ArrayList<>();
        List<PID> newCollectionPids = new ArrayList<>();

        for (MigrationSip sip: sips) {
            destinationPids.add(sip.getDestinationPid());
            newCollectionPids.add(sip.getNewCollectionPid());
        }

        Set<PID> uniqueDestinationPids = new HashSet<>(destinationPids);
        Set<PID> uniqueNewCollectionPids = new HashSet<>(newCollectionPids);

        // there is more than one destination, so no redirect
        if (uniqueDestinationPids.size() > 1) {
            outputLogger.info("There were multiple possible destinations, so chompb can't select one " +
                    "for redirect mapping. You will need to add any redirect mappings to redirect_mappings.csv");
        }
        // there is exactly one distinct new collection, so we'll redirect to it
        if (uniqueNewCollectionPids.size() == 1) {
            csvPrinter.printRecord(cdmCollectionId, null, newCollectionPids.get(0), null);
            return;
        }
        // there are 0 collections or there are multiple collections, so redirect to the boxc destination
        csvPrinter.printRecord(cdmCollectionId, null, destinationPids.get(0), null);
    }
}
