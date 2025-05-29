package edu.unc.lib.boxc.migration.cdm.model;

import java.util.HashMap;
import java.util.Map;

/**
 * Container class for accessing mappings of aspace ref ids to migration objects
 * @author krwong
 */
public class AspaceRefIdInfo {
    public static final String RECORD_ID_FIELD = CdmFieldInfo.CDM_ID;
    public static final String REF_ID_FIELD = "aspaceRefId";
    public static final String[] CSV_HEADERS = {RECORD_ID_FIELD, REF_ID_FIELD};

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
     * @return mapping with matching cdm id, or null if no match
     */
    public Map.Entry<String,String> getMappingByCdmId(String cdmId) {
        for (Map.Entry<String, String> entry : mappings.entrySet()) {
            if (entry.getKey().equals(cdmId)) {
                return entry;
            }
        }
        return null;
    }

//    /**
//     * An individual mapping from a migration object to associated aspace ref id
//     */
//    public static class AspaceRefIdMapping {
//        private String cdmId;
//        private String aspaceRefId;
//
//        public String getCdmId() {
//            return cdmId;
//        }
//
//        public void setCdmId(String cdmId) {
//            this.cdmId = cdmId;
//        }
//
//        public String getAspaceRefId() {
//            return aspaceRefId;
//        }
//
//        public void setAspaceRefId(String aspaceRefId) {
//            this.aspaceRefId = aspaceRefId;
//        }
//    }
}
