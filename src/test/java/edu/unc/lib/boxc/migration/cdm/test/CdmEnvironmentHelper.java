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
    public static final int TEST_HTTP_PORT = 46887;
    public static final String TEST_BASE_URL = "http://localhost:" + TEST_HTTP_PORT;
    public static final int TEST_SSH_PORT = 42222;
    public static final String TEST_SSH_HOST = "localhost";
    public static final String DEFAULT_ENV_ID = "test";

    private CdmEnvironmentHelper() {
    }

    /**
     * @return environment mapping containing test environment
     */
    public static Map<String, CdmEnvironment> getTestMapping() {
        return Map.of(DEFAULT_ENV_ID, getTestEnv());
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
