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
    public static final String GROUPED_WORK_PREFIX = "grp_work:";
    public static final String GROUP_KEY = "group";
    public static final String ID_FIELD = "id";
    public static final String[] CSV_HEADERS = new String[] {
            ID_FIELD, GROUP_KEY };

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
        private String groupKey;

        public String getCdmId() {
            return cdmId;
        }

        public void setCdmId(String cdmId) {
            this.cdmId = cdmId;
        }

        public String getGroupKey() {
            return groupKey;
        }

        public void setGroupKey(String groupKey) {
            if (StringUtils.isBlank(groupKey)) {
                this.groupKey = null;
            } else {
                this.groupKey = groupKey;
            }
        }

    }
}
