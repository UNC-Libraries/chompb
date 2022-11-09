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
package edu.unc.lib.boxc.migration.cdm;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import edu.unc.lib.boxc.migration.cdm.options.SipGenerationOptions;
import edu.unc.lib.boxc.migration.cdm.test.BxcEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.http.HttpStatus;

import java.nio.file.Files;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.junit.Assert.assertTrue;

/**
 * @author bbpennel
 */
public class VerifyPostMigrationCommandIT extends AbstractCommandIT {
    private final static String DEST_UUID = "3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e";
    @Rule
    public WireMockRule wireMockRule = new WireMockRule(options().port(BxcEnvironmentHelper.TEST_HTTP_PORT));

    @Before
    public void setup() throws Exception {
        initProjectAndHelper();
        setupChompbConfig();
        System.setProperty("ENV_CONFIG", chompbConfigPath);
        testHelper.indexExportData("mini_gilmer");
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, null);
        testHelper.populateDescriptions("gilmer_mods1.xml");
        testHelper.populateSourceFiles("276_182_E.tif", "276_183_E.tif", "276_203_E.tif");
    }

    @After
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
