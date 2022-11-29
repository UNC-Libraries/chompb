package edu.unc.lib.boxc.migration.cdm.model;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

/**
 * @author bbpennel
 */
public class SourceFilesInfo {
    public static final String POTENTIAL_MATCHES_FIELD = "potential_matches";
    public static final String SOURCE_FILE_FIELD = "source_file";
    public static final String EXPORT_MATCHING_FIELD = "matching_value";
    public static final String ID_FIELD = "id";
    public static final String[] CSV_HEADERS = new String[] {
            ID_FIELD, EXPORT_MATCHING_FIELD, SOURCE_FILE_FIELD, POTENTIAL_MATCHES_FIELD };

    private List<SourceFileMapping> mappings;

    public SourceFilesInfo() {
        mappings = new ArrayList<>();
    }

    /**
     * @return Mappings of cdm objects to source files
     */
    public List<SourceFileMapping> getMappings() {
        return mappings;
    }

    public void setMappings(List<SourceFileMapping> mappings) {
        this.mappings = mappings;
    }

    /**
     * @param cdmId
     * @return mapping with matching cdm id, or null if no match
     */
    public SourceFileMapping getMappingByCdmId(String cdmId) {
        return this.mappings.stream().filter(m -> m.getCdmId().equals(cdmId)).findFirst().orElseGet(null);
    }

    public static class SourceFileMapping {
        private String cdmId;
        private String matchingValue;
        private Path sourcePath;
        private List<String> potentialMatches;

        public String getCdmId() {
            return cdmId;
        }

        public void setCdmId(String cdmId) {
            this.cdmId = cdmId;
        }

        public String getMatchingValue() {
            return matchingValue;
        }

        public void setMatchingValue(String matchingValue) {
            this.matchingValue = matchingValue;
        }

        public Path getSourcePath() {
            return sourcePath;
        }

        public void setSourcePath(String sourcePath) {
            if (StringUtils.isBlank(sourcePath)) {
                this.sourcePath = null;
            } else {
                this.sourcePath = Paths.get(sourcePath);
            }
        }

        public List<String> getPotentialMatches() {
            return potentialMatches;
        }

        public void setPotentialMatches(String potentialMatches) {
            if (StringUtils.isBlank(potentialMatches)) {
                this.potentialMatches = null;
            } else {
                this.potentialMatches = Arrays.asList(potentialMatches.split(","));
            }
        }

        public void setPotentialMatches(List<String> potentialMatches) {
            this.potentialMatches = potentialMatches;
        }

        public String getPotentialMatchesString() {
            if (potentialMatches == null) {
                return null;
            } else {
                return String.join(",", potentialMatches);
            }
        }
    }
}
