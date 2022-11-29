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
