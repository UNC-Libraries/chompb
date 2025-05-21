package edu.unc.lib.boxc.migration.cdm.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Container class for accessing mappings of aspace ref ids to migration objects
 * @author krwong
 */
public class AspaceRefIdInfo {
    public static final String RECORD_ID_FIELD = CdmFieldInfo.CDM_ID;
    public static final String REF_ID_FIELD = "aspaceRefId";
    public static final String[] CSV_HEADERS = {RECORD_ID_FIELD, REF_ID_FIELD};

    private List<AspaceRefIdMapping> mappings;

    public AspaceRefIdInfo() {
        mappings = new ArrayList<>();
    }

    /**
     * @return Mappings of cdm objects to aspace ref ids
     */
    public List<AspaceRefIdMapping> getMappings() {
        return mappings;
    }

    public void setMappings(List<AspaceRefIdMapping> mappings) {
        this.mappings = mappings;
    }

    /**
     * @param cdmId
     * @return mapping with matching cdm id, or null if no match
     */
    public AspaceRefIdMapping getMappingByCdmId(String cdmId) {
        return this.mappings.stream().filter(m -> m.getCdmId().equals(cdmId)).findFirst().orElse(null);
    }

    /**
     * An individual mapping from a migration object to associated aspace ref id
     */
    public static class AspaceRefIdMapping {
        private String cdmId;
        private String aspaceRefId;

        public String getCdmId() {
            return cdmId;
        }

        public void setCdmId(String cdmId) {
            this.cdmId = cdmId;
        }

        public String getAspaceRefId() {
            return aspaceRefId;
        }

        public void setAspaceRefId(String aspaceRefId) {
            this.aspaceRefId = aspaceRefId;
        }
    }
}
