package edu.unc.lib.boxc.migration.cdm.services;

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.MigrationSip;
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
        List<String> destinationIds = new ArrayList<>();
        List<String> newCollectionIds = new ArrayList<>();

        for (MigrationSip sip: sips) {
            if (sip.getDestinationId() != null) {
                destinationIds.add(sip.getDestinationId());
            }
            if (sip.getNewCollectionId() != null) {
                newCollectionIds.add(sip.getNewCollectionId());
            }
        }

        Set<String> uniqueDestinationIds = new HashSet<>(destinationIds);
        Set<String> uniqueNewCollectionIds = new HashSet<>(newCollectionIds);

        // there is more than one destination, so no redirect
        if (uniqueDestinationIds.size() > 1) {
            outputLogger.info("There were multiple possible destinations, so chompb can't select one " +
                    "for redirect mapping. You will need to add any redirect mappings to redirect_mappings.csv");
            return;
        }
        // there is exactly one distinct new collection, so we'll redirect to it
        if (uniqueNewCollectionIds.size() == 1) {
            csvPrinter.printRecord(cdmCollectionId, null, newCollectionIds.get(0), null);
            return;
        }
        // there are 0 collections or there are multiple collections, so redirect to the boxc destination
        csvPrinter.printRecord(cdmCollectionId, null, destinationIds.get(0), null);
    }
}
