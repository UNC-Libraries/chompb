package edu.unc.lib.boxc.migration.cdm.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Container class for accessing mappings of alt-text body to migration objects.
 * @author krwong
 */
public class AltTextInfo {
    public static final String CDM_ID_FIELD = "cdmid";
    public static final String ALT_TEXT_BODY_FIELD = "alt-text body";
    public static final String[] CSV_HEADERS = new String[] {
            CDM_ID_FIELD, ALT_TEXT_BODY_FIELD };

    private List<AltTextMapping> mappings;

    public AltTextInfo() {
        mappings = new ArrayList<>();
    }

    /**
     * @return Mappings of cdm objects to alt-text body
     */
    public List<AltTextMapping> getMappings() {
        return mappings;
    }

    public void setMappings(List<AltTextMapping> mappings) {
        this.mappings = mappings;
    }

    /**
     * @param cdmId
     * @return mapping with matching cdm id, or null if no match
     */
    public AltTextMapping getMappingByCdmId(String cdmId) {
        return this.mappings.stream().filter(m -> m.getCdmId().equals(cdmId)).findFirst().orElse(null);
    }

    /**
     * An individual mapping from a migration object to associated alt-text body.
     */
    public static class AltTextMapping {
        private String cdmId;
        private String altTextBody;

        public String getCdmId() {
            return cdmId;
        }

        public void setCdmId(String cdmId) {
            this.cdmId = cdmId;
        }

        public String getAltTextBody() {
            return altTextBody;
        }

        public void setAltTextBody(String altTextBody) {
            this.altTextBody = altTextBody;
        }
    }
}
