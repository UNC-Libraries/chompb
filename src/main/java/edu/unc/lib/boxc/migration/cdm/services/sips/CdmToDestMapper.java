package edu.unc.lib.boxc.migration.cdm.services.sips;

import edu.unc.lib.boxc.migration.cdm.model.DestinationSipEntry;
import edu.unc.lib.boxc.migration.cdm.model.DestinationsInfo;

import java.util.HashMap;
import java.util.Map;

/**
 * Tracks mappings between CDM ids and destinations
 *
 * @author bbpennel
 */
public class CdmToDestMapper {
    private Map<String, DestinationSipEntry> cdmId2DestMap = new HashMap<>();

    public void put(String mappingId, DestinationSipEntry destEntry) {
        cdmId2DestMap.put(mappingId, destEntry);
    }

    public DestinationSipEntry getDestinationEntry(String cdmId) {
        DestinationSipEntry entry = cdmId2DestMap.get(cdmId);
        if (entry == null) {
            return cdmId2DestMap.get(DestinationsInfo.DEFAULT_ID);
        } else {
            return entry;
        }
    }
}
