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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.jena.ext.com.google.common.collect.Streams;

/**
 * Information describing the fields present in a CDM collection
 *
 * @author bbpennel
 */
public class CdmFieldInfo {
    public static final String CDM_ID = "dmrecord";
    public static final String CDM_CREATED = "dmcreated";
    public static final Set<String> IGNORE_FIELDS = new HashSet<>(Arrays.asList(
            "dmoclcno", "dmad1", "dmad2"));
    public static final List<String> RESERVED_FIELDS = Arrays.asList(
            CDM_ID, CDM_CREATED, "dmmodified", "find");

    private List<CdmFieldEntry> fields;
    private List<String> exportFields;
    private List<String> configuredFields;

    public CdmFieldInfo() {
        fields = new ArrayList<>();
    }

    public List<CdmFieldEntry> getFields() {
        return fields;
    }

    public void setFields(List<CdmFieldEntry> fields) {
        this.fields = fields;
    }

    /**
     * @return List of all the configured export fields. Does not include reserved CDM fields.
     */
    public List<String> listConfiguredFields() {
        if (configuredFields == null) {
            configuredFields = streamConfiguredFields().collect(Collectors.toList());
        }
        return configuredFields;
    }

    /**
     * @return List of all the exported fields, including reserved CDM fields
     */
    public List<String> listAllExportFields() {
        if (exportFields == null) {
            exportFields = streamConfiguredFields().collect(Collectors.toList());
        }
        return exportFields;
    }

    private Stream<String> streamConfiguredFields() {
        return getFields().stream()
                .filter(f -> !f.getSkipExport() && !IGNORE_FIELDS.contains(f.getExportAs()))
                .map(CdmFieldEntry::getExportAs);
    }

    /**
     * Individual field entry
     * @author bbpennel
     */
    public static class CdmFieldEntry {
        private String nickName;
        private String exportAs;
        private String description;
        private Boolean skipExport;
        private String cdmRequired;
        private String cdmSearchable;
        private String cdmHidden;
        private String cdmVocab;
        private String cdmDcMapping;

        public String getNickName() {
            return nickName;
        }

        public void setNickName(String nickName) {
            this.nickName = nickName;
        }

        public String getExportAs() {
            return exportAs;
        }

        public void setExportAs(String exportAs) {
            this.exportAs = exportAs;
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }

        public Boolean getSkipExport() {
            return skipExport;
        }

        public void setSkipExport(Boolean skipExport) {
            this.skipExport = skipExport;
        }

        public String getCdmRequired() {
            return cdmRequired;
        }

        public void setCdmRequired(String cdmRequired) {
            this.cdmRequired = cdmRequired;
        }

        public String getCdmSearchable() {
            return cdmSearchable;
        }

        public void setCdmSearchable(String cdmSearchable) {
            this.cdmSearchable = cdmSearchable;
        }

        public String getCdmHidden() {
            return cdmHidden;
        }

        public void setCdmHidden(String cdmHidden) {
            this.cdmHidden = cdmHidden;
        }

        public String getCdmVocab() {
            return cdmVocab;
        }

        public void setCdmVocab(String cdmVocab) {
            this.cdmVocab = cdmVocab;
        }

        public String getCdmDcMapping() {
            return cdmDcMapping;
        }

        public void setCdmDcMapping(String cdmDcMapping) {
            this.cdmDcMapping = cdmDcMapping;
        }
    }
}
