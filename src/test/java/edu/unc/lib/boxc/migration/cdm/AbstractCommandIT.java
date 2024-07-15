package edu.unc.lib.boxc.migration.cdm;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.ChompbConfigService;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.test.BxcEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.SipServiceHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import static org.junit.jupiter.api.Assertions.fail;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author bbpennel
 */
public class AbstractCommandIT extends AbstractOutputTest {
    private static final Logger log = getLogger(AbstractCommandIT.class);
    protected String defaultCollectionId = "my_coll";
    protected final static String PROJECT_ID = "my_proj";
    protected final static String USERNAME = "theuser";
    private final String initialUser = System.getProperty("user.name");
    private static final String PROJECT_SOURCE = "cdm";

    protected CommandLine migrationCommand;
    protected SipServiceHelper testHelper;

    protected MigrationProject project;
    protected String chompbConfigPath;

    @AfterEach
    public void resetProps() {
        System.setProperty("user.name", initialUser);
    }

    @BeforeEach
    public void baseSetUp() throws Exception {
        System.setProperty("user.name", USERNAME);
        migrationCommand = new CommandLine(new CLIMain());
    }

    protected void initTestHelper() throws IOException {
        testHelper = new SipServiceHelper(project, tmpFolder);
    }

    protected void initProject() throws IOException {
        project = MigrationProjectFactory.createMigrationProject(baseDir, PROJECT_ID, defaultCollectionId, USERNAME,
                CdmEnvironmentHelper.DEFAULT_ENV_ID, BxcEnvironmentHelper.DEFAULT_ENV_ID, PROJECT_SOURCE);
    }

    protected void initProjectAndHelper() throws IOException {
        initProject();
        initTestHelper();
    }

    protected void setupChompbConfig() throws IOException {
        var configPath = tmpFolder.resolve("config.json");
        var config = new ChompbConfigService.ChompbConfig();
        config.setCdmEnvironments(CdmEnvironmentHelper.getTestMapping());
        config.setBxcEnvironments(BxcEnvironmentHelper.getTestMapping());
        ObjectMapper mapper = new ObjectMapper();
        mapper.writeValue(Files.newOutputStream(configPath), config);
        chompbConfigPath = configPath.toString();
    }

    protected void executeExpectSuccess(List<String> args) {
        executeExpectSuccess(args.toArray(new String[0]));
    }

    protected void executeExpectSuccess(String[] args) {
        int result = migrationCommand.execute(args);
        output = out.toString();
        if (result != 0) {
            System.setOut(originalOut);
            // Can't see the output from the command without this
            System.out.println(output);
            fail("Expected command to result in success: " + String.join(" ", args) + "\nWith output:\n" + output);
        }
    }

    protected void executeExpectFailure(String[] args) {
        int result = migrationCommand.execute(args);
        output = out.toString();
        if (result == 0) {
            System.setOut(originalOut);
            log.error(output);
            fail("Expected command to result in failure: " + String.join(" ", args));
        }
    }
}
