package edu.unc.lib.boxc.migration.cdm.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

/**
 * @author bbpennel
 */
public class GroupMappingInfo {
    public static final String GROUPED_WORK_PREFIX = "grp:";
    public static final String GROUP_KEY1 = "group1";
    public static final String GROUP_KEY2 = "group2";
    public static final String ID_FIELD = "id";
    public static final String[] CSV_HEADERS = new String[] {
            ID_FIELD, GROUP_KEY1, GROUP_KEY2 };

    private Map<String, List<String>> groupedMappings = new HashMap<>();
    private List<GroupMapping> mappings = new ArrayList<>();

    /**
     * @return Mapping of group keys to object ids.
     */
    public Map<String, List<String>> getGroupedMappings() {
        return groupedMappings;
    }

    public void setGroupedMappings(Map<String, List<String>> groupedMappings) {
        this.groupedMappings = groupedMappings;
    }

    /**
     * @return Raw list of mappings between objects and groups
     */
    public List<GroupMapping> getMappings() {
        return mappings;
    }

    public void setMappings(List<GroupMapping> mappings) {
        this.mappings = mappings;
    }

    /**
     * @param cdmId
     * @return mapping with matching cdm id, or null if no match
     */
    public GroupMapping getMappingByCdmId(String cdmId) {
        return mappings.stream().filter(m -> m.getCdmId().equals(cdmId)).findFirst().orElse(null);
    }

    public static class GroupMapping {
        private String cdmId;
        private String groupKey1;
        private String groupKey2;

        public String getCdmId() {
            return cdmId;
        }

        public void setCdmId(String cdmId) {
            this.cdmId = cdmId;
        }

        public String getGroupKey1() {
            return groupKey1;
        }

        public void setGroupKey1(String groupKey1) {
            if (StringUtils.isBlank(groupKey1)) {
                this.groupKey1 = null;
            } else {
                this.groupKey1 = groupKey1;
            }
        }

        public String getGroupKey2() {
            return groupKey2;
        }

        public void setGroupKey2(String groupKey2) {
            if (StringUtils.isBlank(groupKey2)) {
                this.groupKey2 = null;
            } else {
                this.groupKey2 = groupKey2;
            }
        }
    }
}
