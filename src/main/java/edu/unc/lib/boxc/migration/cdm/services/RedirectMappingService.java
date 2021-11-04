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

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.GroupMappingInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Files;

/**
 * Service for generating a redirect mapping csv with the following information:
 * CDM Collection ID, CDM Object ID, Box-c Work ID, Box-c File ID
 *
 * @author snluong
 */
public class RedirectMappingService {
    private MigrationProject project;
    private CSVPrinter csvPrinter;
    public static final String[] CSV_HEADERS = new String[] {
            "cdm_collection_id", "cdm_object_id", "boxc_work_id", "boxc_file_id" };

    public RedirectMappingService(MigrationProject project) {
        this.project = project;
    }

    /**
     * Opens RedirectMapping CSV and sets its path
     */
    public void init() {
        try {
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
     * @param boxcWorkId
     * @param boxcFileId
     */
    public void addRow(String cdmObjectId, String boxcWorkId, String boxcFileId) {
        String cdmCollectionId = project.getProjectProperties().getCdmCollectionId();
        try {
            csvPrinter.printRecord(cdmCollectionId, cdmObjectId, boxcWorkId, boxcFileId);
        } catch (IOException e) {
            throw new MigrationException("Error adding row to redirect mapping CSV", e);
        }
    }
}
