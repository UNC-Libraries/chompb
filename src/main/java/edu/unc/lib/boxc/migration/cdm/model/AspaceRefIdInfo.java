package edu.unc.lib.boxc.migration.cdm.model;

import java.util.HashMap;
import java.util.Map;

/**
 * Container class for accessing mappings of aspace ref ids to migration objects
 * @author krwong
 */
public class AspaceRefIdInfo {
    public static final String RECORD_ID_FIELD = CdmFieldInfo.CDM_ID;
    public static final String HOOK_ID_FIELD = "hookId";
    public static final String REF_ID_FIELD = "aspaceRefId";
    public static final String[] CSV_HEADERS = {RECORD_ID_FIELD, HOOK_ID_FIELD, REF_ID_FIELD};

    private Map<String, String> mappings;

    public AspaceRefIdInfo() {
        mappings = new HashMap<>();
    }

    /**
     * @return Mappings of cdm objects to aspace ref ids
     */
    public Map<String, String> getMappings() {
        return mappings;
    }

    public void setMappings(Map<String, String> mappings) {
        this.mappings = mappings;
    }

    /**
     * @param cdmId
     * @return aspaceRefId, or null if no match
     */
    public String getRefIdByRecordId(String cdmId) {
        return mappings.get(cdmId);
    }

}
