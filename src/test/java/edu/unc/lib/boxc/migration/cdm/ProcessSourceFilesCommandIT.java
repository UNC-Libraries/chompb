package edu.unc.lib.boxc.migration.cdm;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.unc.lib.boxc.migration.cdm.services.ChompbConfigService;
import edu.unc.lib.boxc.migration.cdm.test.BxcEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.TestSshServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author bbpennel
 */
public class ProcessSourceFilesCommandIT extends AbstractCommandIT {
    private TestSshServer testSshServer;
    private static final Path CLIENT_KEY_PATH = Paths.get("src/test/resources/test_client_key");

    @BeforeEach
    public void setup() throws Exception {
        initProjectAndHelper();
        testSshServer = new TestSshServer();
        testSshServer.setClientKeyPath(CLIENT_KEY_PATH);
        testSshServer.startServer();
        setupChompbConfig();
    }

    @AfterEach
    public void tearDown() throws Exception {
        testSshServer.stopServer();
    }

    @Override
    protected void setupChompbConfig() throws IOException {
        var configPath = tmpFolder.resolve("config.json");
        var config = new ChompbConfigService.ChompbConfig();
        config.setCdmEnvironments(CdmEnvironmentHelper.getTestMapping());
        var bxcEnvs = BxcEnvironmentHelper.getTestMapping();
        config.setBxcEnvironments(bxcEnvs);
        var bxcEnv = bxcEnvs.get(BxcEnvironmentHelper.DEFAULT_ENV_ID);
        bxcEnv.setBoxctronScriptHost("127.0.0.1");
        bxcEnv.setBoxctronTransferHost("127.0.0.1");
        bxcEnv.setBoxctronPort(42222);
        bxcEnv.setBoxctronSshUser("testuser");
        bxcEnv.setBoxctronKeyPath(CLIENT_KEY_PATH);
        bxcEnv.setBoxctronAdminEmail("chompb@example.com");
        bxcEnv.setBoxctronOutputServer("chompb.example.com");
        bxcEnv.setBoxctronOutputBasePath(tmpFolder);
        bxcEnv.setBoxctronRemoteJobScriptsPath(tmpFolder.resolve("scripts"));
        bxcEnv.setBoxctronRemoteProjectsPath(tmpFolder.resolve("remote_projects"));
        ObjectMapper mapper = new ObjectMapper();
        mapper.writeValue(Files.newOutputStream(configPath), config);
        chompbConfigPath = configPath.toString();
    }

    @Test
    public void testProcessSourceFilesInvalidAction() {
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "--env-config", chompbConfigPath,
                "process_source_files",
                "-a", "garbo"};
        executeExpectFailure(args);
        assertOutputContains("Invalid action name provided: garbo");
    }

    @Test
    public void testProcessSourceFilesValid() {
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "--env-config", chompbConfigPath,
                "process_source_files",
                "-a", "velocicroptor"};
        executeExpectSuccess(args);
        assertOutputContains("Completed velocicroptor job to process source files");
    }
}
