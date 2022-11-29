package edu.unc.lib.boxc.migration.cdm.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.unc.lib.boxc.migration.cdm.model.CdmEnvironment;
import edu.unc.lib.boxc.migration.cdm.model.BxcEnvironment;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/**
 * Service for interactions with chompb application environment configuration
 *
 * @author bbpennel
 */
public class ChompbConfigService {
    private Path envConfigPath;
    private ChompbConfig config;

    public ChompbConfig getConfig() throws IOException {
        if (config == null) {
            ObjectMapper mapper = new ObjectMapper();
            config = mapper.readValue(Files.newInputStream(envConfigPath), ChompbConfig.class);
        }
        return config;
    }

    public void setEnvConfigPath(Path envConfigPath) {
        this.envConfigPath = envConfigPath;
    }

    public static class ChompbConfig {
        private Map<String, CdmEnvironment> cdmEnvironments;
        private Map<String, BxcEnvironment> bxcEnvironments;

        public Map<String, CdmEnvironment> getCdmEnvironments() {
            return cdmEnvironments;
        }

        public void setCdmEnvironments(Map<String, CdmEnvironment> cdmEnvironments) {
            this.cdmEnvironments = cdmEnvironments;
        }

        public Map<String, BxcEnvironment> getBxcEnvironments() {
            return bxcEnvironments;
        }

        public void setBxcEnvironments(Map<String, BxcEnvironment> bxcEnvironments) {
            this.bxcEnvironments = bxcEnvironments;
        }
    }
}
