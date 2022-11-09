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

import edu.unc.lib.boxc.migration.cdm.model.BxcEnvironment;

import java.util.Map;

/**
 * Test helpers for working with bxc environments
 *
 * @author bbpennel
 */
public class BxcEnvironmentHelper {
    public static final String DEFAULT_ENV_ID = "test";
    public static final int TEST_HTTP_PORT = 46887;
    public static final String TEST_BASE_URL = "http://localhost:" + TEST_HTTP_PORT + "/bxc/";

    /**
     * @return environment mapping containing test environment
     */
    public static Map<String, BxcEnvironment> getTestMapping() {
        return Map.of(DEFAULT_ENV_ID, getTestEnv());
    }

    /**
     * @return BxcEnvironment for a test environment
     */
    public static BxcEnvironment getTestEnv() {
        var testEnv = new BxcEnvironment();
        testEnv.setHttpBaseUrl(TEST_BASE_URL);
        return testEnv;
    }
}
