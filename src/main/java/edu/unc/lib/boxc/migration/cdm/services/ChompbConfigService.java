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
package edu.unc.lib.boxc.migration.cdm.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.unc.lib.boxc.migration.cdm.model.CdmEnvironment;

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

        public Map<String, CdmEnvironment> getCdmEnvironments() {
            return cdmEnvironments;
        }

        public void setCdmEnvironments(Map<String, CdmEnvironment> cdmEnvironments) {
            this.cdmEnvironments = cdmEnvironments;
        }
    }
}
