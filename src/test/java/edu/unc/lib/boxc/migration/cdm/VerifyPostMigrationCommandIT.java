package edu.unc.lib.boxc.migration.cdm;

import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import edu.unc.lib.boxc.migration.cdm.options.SipGenerationOptions;
import edu.unc.lib.boxc.migration.cdm.test.BxcEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

import java.nio.file.Files;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author bbpennel
 */
@WireMockTest(httpPort = BxcEnvironmentHelper.TEST_HTTP_PORT)
public class VerifyPostMigrationCommandIT extends AbstractCommandIT {
    private final static String DEST_UUID = "3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e";

    @BeforeEach
    public void setup() throws Exception {
        initProjectAndHelper();
        setupChompbConfig();
        System.setProperty("ENV_CONFIG", chompbConfigPath);
        testHelper.indexExportData("mini_gilmer");
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, null);
        testHelper.populateDescriptions("gilmer_mods1.xml");
        testHelper.populateSourceFiles("276_182_E.tif", "276_183_E.tif", "276_203_E.tif");
    }

    @AfterEach
    public void cleanup() {
        System.clearProperty("ENV_CONFIG");
    }

    @Test
    public void noReportTest() throws Exception {
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "verify_migration" };
        executeExpectFailure(args);
        assertOutputContains("Post migration report has not been generated yet");
    }

    @Test
    public void successTest() throws Exception {
        stubFor(get(urlMatching("/bxc/record/.*"))
                .willReturn(aResponse()
                        .withStatus(HttpStatus.OK.value())));

        generateSip();

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "verify_migration" };
        executeExpectSuccess(args);
        assertOutputContains("Checked 6 out of 6 objects in the report");
        assertOutputContains("Success! No problems were found");
        assertTrue(Files.exists(project.getPostMigrationReportPath()));
        assertTrue(Files.readString(project.getPostMigrationReportPath()).contains(HttpStatus.OK.name()));
    }

    @Test
    public void errorsTest() throws Exception {
        stubFor(get(urlMatching("/bxc/record/.*"))
                .willReturn(aResponse()
                        .withStatus(HttpStatus.NOT_FOUND.value())));

        generateSip();

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "verify_migration" };
        executeExpectFailure(args);
        assertOutputContains("Errors encountered for 6 objects, see report for details");
        assertTrue(Files.exists(project.getPostMigrationReportPath()));
        assertTrue(Files.readString(project.getPostMigrationReportPath()).contains(HttpStatus.NOT_FOUND.name()));
    }

    private void generateSip() {
        var sipService = testHelper.createSipsService();
        var sipOptions = new SipGenerationOptions();
        sipOptions.setUsername("user");
        sipService.generateSips(sipOptions);
    }
}
