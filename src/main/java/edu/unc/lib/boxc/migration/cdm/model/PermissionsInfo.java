package edu.unc.lib.boxc.migration.cdm.model;

import static edu.unc.lib.boxc.auth.api.AccessPrincipalConstants.PUBLIC_PRINC;
import static edu.unc.lib.boxc.auth.api.AccessPrincipalConstants.AUTHENTICATED_PRINC;

import java.util.ArrayList;
import java.util.List;

/**
 * Permission mapping information for a project
 * @author krwong
 */
public class PermissionsInfo {
    public static final String DEFAULT_ID = "default";
    public static final String ID_FIELD = "id";
    public static final String OBJECT_TYPE = "object_type";
    public static final String[] CSV_HEADERS = new String[] {
            ID_FIELD, OBJECT_TYPE, PUBLIC_PRINC, AUTHENTICATED_PRINC };

    private List<PermissionMapping> mappings;

    public PermissionsInfo() {
        mappings = new ArrayList<>();
    }

    public List<PermissionMapping> getMappings() {
        return mappings;
    }

    public void setMappings(List<PermissionMapping> mappings) {
        this.mappings = mappings;
    }

    /**
     * @return default mapping, or none if no match
     */
    public PermissionsInfo.PermissionMapping getDefaultMapping() {
        return this.mappings.stream().filter(m -> m.getId().equals(DEFAULT_ID)).findFirst().orElse(null);
    }

    /**
     * @param cdmId
     * @return mapping with matching cdm id, or default mapping if no match
     */
    public PermissionsInfo.PermissionMapping getMappingByCdmId(String cdmId) {
        return this.mappings.stream().filter(m -> m.getId().equals(cdmId)).findFirst().orElse(getDefaultMapping());
    }

    /**
     * An individual permission mapping
     * @author krwong
     */
    public static class PermissionMapping {
        private String id;
        private String everyone;
        private String authenticated;

        public PermissionMapping() {
        }

        public PermissionMapping(String id, String everyone, String authenticated) {
            this.id = id;
            this.everyone = everyone;
            this.authenticated = authenticated;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getEveryone() {
            return everyone;
        }

        public void setEveryone(String everyone) {
            this.everyone = everyone;
        }

        public String getAuthenticated() {
            return authenticated;
        }

        public void setAuthenticated(String authenticated) {
            this.authenticated = authenticated;
        }
    }
}
