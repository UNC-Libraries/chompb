package edu.unc.lib.boxc.migration.cdm.model;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

/**
 * @author bbpennel
 */
public class SourceFilesInfo {
    public static final String SEPARATOR = "|";
    public static final String ESCAPED_SEPARATOR = "\\|";
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
        private List<Path> sourcePaths;
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

        public List<Path> getSourcePaths() {
            return sourcePaths;
        }

        public Path getFirstSourcePath() {
            return (sourcePaths == null) ? null : sourcePaths.get(0);
        }

        public void setSourcePaths(List<Path> sourcePaths) {
            this.sourcePaths = sourcePaths;
        }

        public void setSourcePaths(String sourcePaths) {
            if (StringUtils.isBlank(sourcePaths)) {
                this.sourcePaths = null;
            } else {
                this.sourcePaths = Arrays.stream(sourcePaths.split(ESCAPED_SEPARATOR))
                                        .map(Paths::get)
                                        .collect(Collectors.toList());
            }
        }

        public String getSourcePathString() {
            if (sourcePaths == null) {
                return null;
            } else {
                return sourcePaths.stream().map(Object::toString).collect(Collectors.joining(SEPARATOR));
            }
        }

        public List<String> getPotentialMatches() {
            return potentialMatches;
        }

        public void setPotentialMatches(String potentialMatches) {
            if (StringUtils.isBlank(potentialMatches)) {
                this.potentialMatches = null;
            } else {
                this.potentialMatches = Arrays.asList(potentialMatches.split(ESCAPED_SEPARATOR));
            }
        }

        public void setPotentialMatches(List<String> potentialMatches) {
            this.potentialMatches = potentialMatches;
        }

        public String getPotentialMatchesString() {
            if (potentialMatches == null) {
                return null;
            } else {
                return String.join(SEPARATOR, potentialMatches);
            }
        }
    }
}
