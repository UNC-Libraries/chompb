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
package edu.unc.lib.boxc.migration.cdm.test;

import edu.unc.lib.boxc.migration.cdm.model.CdmEnvironment;

import java.nio.file.Paths;
import java.util.Map;

/**
 * Test helpers for working with cdm environment
 *
 * @author bbpennel
 */
public class CdmEnvironmentHelper {
    public static final int TEST_HTTP_PORT = 46888;
    public static final String TEST_BASE_URL = "http://localhost:" + TEST_HTTP_PORT;
    public static final int TEST_SSH_PORT = 42222;
    public static final String TEST_SSH_HOST = "localhost";

    private CdmEnvironmentHelper() {
    }

    /**
     * @return environment mapping containing test environment
     */
    public static Map<String, CdmEnvironment> getTestMapping() {
        return Map.of("test", getTestEnv());
    }

    /**
     * @return CdmEnvironment for a test environment
     */
    public static CdmEnvironment getTestEnv() {
        var testEnv = new CdmEnvironment();
        testEnv.setHttpBaseUrl(TEST_BASE_URL);
        testEnv.setSshPort(TEST_SSH_PORT);
        testEnv.setSshHost(TEST_SSH_HOST);
        testEnv.setSshDownloadBasePath(Paths.get("src/test/resources/descriptions").toString());
        return testEnv;
    }
}
