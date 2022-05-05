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
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo.CdmFieldEntry;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.CdmFieldService;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.test.TestSshServer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author bbpennel
 */
public class CdmExportCommandIT extends AbstractCommandIT {

    private final static String COLLECTION_ID = "gilmer";
    private final static String PASSWORD = "supersecret";
    private final static String BODY_RESP = "<xml>record</xml>";
    private final static String PAGESIZE = "50";

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(options().dynamicPort());
    private TestSshServer testSshServer;

    private CdmFieldService fieldService;
    private String cdmBaseUrl;

    @Before
    public void setUp() throws Exception {
        fieldService = new CdmFieldService();
        cdmBaseUrl = "http://localhost:" + wireMockRule.port();

        stubFor(post(urlEqualTo("/cgi-bin/admin/exportxml.exe"))
                .willReturn(aResponse()
                        .withStatus(200)));
        stubFor(get(urlEqualTo("/cgi-bin/admin/getfile.exe?CISOMODE=1&CISOFILE=/gilmer/index/description/export.xml"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/octet-stream")
                        .withBody(BODY_RESP)));
        testSshServer = new TestSshServer();
        testSshServer.setPassword(PASSWORD);
        testSshServer.startServer();
    }

    @After
    public void cleanup() throws Exception {
        testSshServer.stopServer();
    }

    private String[] exportArgs(Path projPath, String... extras) {
        String[] defaultArgs = new String[] {
                "-w", projPath.toString(),
                "export",
                "-D", Paths.get("src/test/resources/descriptions").toAbsolutePath().toString(),
                "-P", "2222",
                "--cdm-url", cdmBaseUrl,
                "-p", PASSWORD};
        return ArrayUtils.addAll(defaultArgs, extras);
    }

    @Test
    public void exportValidProjectTest() throws Exception {
        stubFor(get(urlEqualTo("/cgi-bin/admin/getfile.exe?CISOMODE=1&CISOFILE=/gilmer/index/description/export.xml"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/octet-stream")
                        .withBody(IOUtils.toString(getClass().getResourceAsStream("/sample_exports/gilmer/export_all.xml"), StandardCharsets.UTF_8))));

        Path projPath = createProject();

        String[] args = exportArgs(projPath);
        executeExpectSuccess(args);

        MigrationProject project = MigrationProjectFactory.loadMigrationProject(projPath);

        assertTrue("Export folder not created", Files.exists(project.getExportPath()));
        assertEquals(IOUtils.toString(getClass().getResourceAsStream("/sample_exports/gilmer/export_all.xml"), StandardCharsets.UTF_8), FileUtils.readFileToString(
                project.getExportPath().resolve("export_1.xml").toFile(), StandardCharsets.UTF_8));
    }

    @Test
    public void noUsernameTest() throws Exception {
        System.clearProperty("user.name");
        Path projPath = createProject();

        String[] args = exportArgs(projPath);
        executeExpectFailure(args);

        MigrationProject project = MigrationProjectFactory.loadMigrationProject(projPath);
        assertFalse("Description folder should not be created", Files.exists(project.getExportPath()));
        assertOutputContains("Must provided a CDM username");
    }

    @Test
    public void noPasswordTest() throws Exception {
        Path projPath = createProject();

        String[] args = new String[] {
                "-w", projPath.toString(),
                "export",
                "--cdm-url", cdmBaseUrl };
        executeExpectFailure(args);

        MigrationProject project = MigrationProjectFactory.loadMigrationProject(projPath);
        assertFalse("Description folder should not be created", Files.exists(project.getExportPath()));
        assertOutputContains("Must provided a CDM password");
    }

    @Test
    public void lessThanMinPageSizeTest() throws Exception {
        Path projPath = createProject();

        String[] args = exportArgs(projPath,
                "-n", "0");
        executeExpectFailure(args);

        MigrationProject project = MigrationProjectFactory.loadMigrationProject(projPath);
        assertFalse("Export folder must not exist", Files.exists(project.getExportPath()));
        assertOutputContains("Page size must be between 1 and 5000");
    }

    @Test
    public void greaterThanMaxPageSizeTest() throws Exception {
        Path projPath = createProject();

        String[] args = exportArgs(projPath,
                "-n", "50000");
        executeExpectFailure(args);

        MigrationProject project = MigrationProjectFactory.loadMigrationProject(projPath);
        assertFalse("Export folder must not exist", Files.exists(project.getExportPath()));
        assertOutputContains("Page size must be between 1 and 5000");
    }

    @Test
    public void errorResponseTest() throws Exception {
        stubFor(post(urlEqualTo("/cgi-bin/admin/exportxml.exe"))
                .willReturn(aResponse()
                        .withStatus(400)));

        Path projPath = createProject();

        String[] args = exportArgs(projPath);
        executeExpectFailure(args);

        MigrationProject project = MigrationProjectFactory.loadMigrationProject(projPath);

        assertFalse("Export file should not be created", Files.exists(project.getExportPath().resolve("export_1.xml")));
        assertOutputContains("Failed to request export");
    }

    @Test
    public void exportLessThanPageSizeTest() throws Exception {
        stubFor(get(urlEqualTo("/cgi-bin/admin/getfile.exe?CISOMODE=1&CISOFILE=/gilmer/index/description/export.xml"))
                .inScenario("Multiple Exports")
                .whenScenarioStateIs(Scenario.STARTED)
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/octet-stream")
                        .withBody(IOUtils.toString(getClass().getResourceAsStream("/sample_exports/gilmer/export_1.xml"), StandardCharsets.UTF_8)))
                .willSetStateTo("Page 2"));
        stubFor(get(urlEqualTo("/cgi-bin/admin/getfile.exe?CISOMODE=1&CISOFILE=/gilmer/index/description/export.xml"))
                .inScenario("Multiple Exports")
                .whenScenarioStateIs("Page 2")
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/octet-stream")
                        .withBody(IOUtils.toString(getClass().getResourceAsStream("/sample_exports/gilmer/export_2.xml"), StandardCharsets.UTF_8)))
                .willSetStateTo("Page 3"));
        stubFor(get(urlEqualTo("/cgi-bin/admin/getfile.exe?CISOMODE=1&CISOFILE=/gilmer/index/description/export.xml"))
                .inScenario("Multiple Exports")
                .whenScenarioStateIs("Page 3")
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/octet-stream")
                        .withBody(IOUtils.toString(getClass().getResourceAsStream("/sample_exports/gilmer/export_3.xml"), StandardCharsets.UTF_8)))
                .willSetStateTo("Page 4"));
        stubFor(get(urlEqualTo("/cgi-bin/admin/getfile.exe?CISOMODE=1&CISOFILE=/gilmer/index/description/export.xml"))
                .inScenario("Multiple Exports")
                .whenScenarioStateIs("Page 4")
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/octet-stream")
                        .withBody(IOUtils.toString(getClass().getResourceAsStream("/sample_exports/gilmer/export_4.xml"), StandardCharsets.UTF_8))));

        Path projPath = createProject();

        String[] args = exportArgs(projPath,
                "-n", PAGESIZE);
        executeExpectSuccess(args);

        MigrationProject project = MigrationProjectFactory.loadMigrationProject(projPath);

        assertTrue("Export folder not created", Files.exists(project.getExportPath()));
        assertEquals(IOUtils.toString(getClass().getResourceAsStream("/sample_exports/gilmer/export_1.xml"), StandardCharsets.UTF_8), FileUtils.readFileToString(
                project.getExportPath().resolve("export_1.xml").toFile(), StandardCharsets.UTF_8));
        assertEquals(IOUtils.toString(getClass().getResourceAsStream("/sample_exports/gilmer/export_2.xml"), StandardCharsets.UTF_8), FileUtils.readFileToString(
                project.getExportPath().resolve("export_2.xml").toFile(), StandardCharsets.UTF_8));
        assertEquals(IOUtils.toString(getClass().getResourceAsStream("/sample_exports/gilmer/export_3.xml"), StandardCharsets.UTF_8), FileUtils.readFileToString(
                project.getExportPath().resolve("export_3.xml").toFile(), StandardCharsets.UTF_8));
        assertEquals(IOUtils.toString(getClass().getResourceAsStream("/sample_exports/gilmer/export_4.xml"), StandardCharsets.UTF_8), FileUtils.readFileToString(
                project.getExportPath().resolve("export_4.xml").toFile(), StandardCharsets.UTF_8));
    }

    @Test
    public void multipageExportErrorAndResumeTest() throws Exception {
        stubFor(get(urlEqualTo("/cgi-bin/admin/getfile.exe?CISOMODE=1&CISOFILE=/gilmer/index/description/export.xml"))
                .inScenario("Multiple Exports")
                .whenScenarioStateIs(Scenario.STARTED)
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/octet-stream")
                        .withBody(IOUtils.toString(getClass().getResourceAsStream("/sample_exports/gilmer/export_1.xml"), StandardCharsets.UTF_8)))
                .willSetStateTo("Page 2"));
        stubFor(get(urlEqualTo("/cgi-bin/admin/getfile.exe?CISOMODE=1&CISOFILE=/gilmer/index/description/export.xml"))
                .inScenario("Multiple Exports")
                .whenScenarioStateIs("Page 2")
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/octet-stream")
                        .withBody(IOUtils.toString(getClass().getResourceAsStream("/sample_exports/gilmer/export_2.xml"), StandardCharsets.UTF_8)))
                .willSetStateTo("Page 3"));

        // Error on the third page
        stubFor(get(urlEqualTo("/cgi-bin/admin/getfile.exe?CISOMODE=1&CISOFILE=/gilmer/index/description/export.xml"))
                .inScenario("Multiple Exports")
                .whenScenarioStateIs("Page 3")
                .willReturn(aResponse()
                        .withStatus(400)));

        stubFor(get(urlEqualTo("/cgi-bin/admin/getfile.exe?CISOMODE=1&CISOFILE=/gilmer/index/description/export.xml"))
                .inScenario("Multiple Exports")
                .whenScenarioStateIs("Page 4")
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/octet-stream")
                        .withBody(IOUtils.toString(getClass().getResourceAsStream("/sample_exports/gilmer/export_4.xml"), StandardCharsets.UTF_8))));

        Path projPath = createProject();

        String[] args = exportArgs(projPath,
                "-n", PAGESIZE);
        executeExpectFailure(args);

        MigrationProject project = MigrationProjectFactory.loadMigrationProject(projPath);
        assertExportFilesPresent(project, "export_1.xml", "export_2.xml");

        // Correct the third page
        stubFor(get(urlEqualTo("/cgi-bin/admin/getfile.exe?CISOMODE=1&CISOFILE=/gilmer/index/description/export.xml"))
                .inScenario("Multiple Exports")
                .whenScenarioStateIs("Page 3")
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/octet-stream")
                        .withBody(IOUtils.toString(getClass().getResourceAsStream("/sample_exports/gilmer/export_3.xml"), StandardCharsets.UTF_8)))
                .willSetStateTo("Page 4"));

        String[] args2 = new String[] {
                "-w", projPath.toString(),
                "export",
                "--cdm-url", cdmBaseUrl,
                "-p", PASSWORD,
                "-n", PAGESIZE};
        executeExpectSuccess(args2);
        assertOutputMatches(".*Resuming incomplete export started [0-9\\-T.:Z]+ from where it left off.*");
        assertOutputContains("Listing of object IDs complete");
        assertOutputContains("Resuming export of object records");

        assertExportFilesPresent(project, "export_1.xml", "export_2.xml", "export_3.xml", "export_4.xml");
        assertEquals(IOUtils.toString(getClass().getResourceAsStream("/sample_exports/gilmer/export_1.xml"), StandardCharsets.UTF_8), FileUtils.readFileToString(
                project.getExportPath().resolve("export_1.xml").toFile(), StandardCharsets.UTF_8));
        assertEquals(IOUtils.toString(getClass().getResourceAsStream("/sample_exports/gilmer/export_2.xml"), StandardCharsets.UTF_8), FileUtils.readFileToString(
                project.getExportPath().resolve("export_2.xml").toFile(), StandardCharsets.UTF_8));
        assertEquals(IOUtils.toString(getClass().getResourceAsStream("/sample_exports/gilmer/export_3.xml"), StandardCharsets.UTF_8), FileUtils.readFileToString(
                project.getExportPath().resolve("export_3.xml").toFile(), StandardCharsets.UTF_8));
        assertEquals(IOUtils.toString(getClass().getResourceAsStream("/sample_exports/gilmer/export_4.xml"), StandardCharsets.UTF_8), FileUtils.readFileToString(
                project.getExportPath().resolve("export_4.xml").toFile(), StandardCharsets.UTF_8));
    }

    @Test
    public void rerunCompletedExportTest() throws Exception {
        String originalExportBody = IOUtils.toString(getClass()
                .getResourceAsStream("/sample_exports/gilmer/export_all.xml"), StandardCharsets.UTF_8);
        stubFor(get(urlEqualTo("/cgi-bin/admin/getfile.exe?CISOMODE=1&CISOFILE=/gilmer/index/description/export.xml"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/octet-stream")
                        .withBody(originalExportBody)));

        Path projPath = createProject();

        String[] args = exportArgs(projPath);
        executeExpectSuccess(args);

        // Change response for export so it will be clear if a new export occurs
        String modifiedBody = originalExportBody + "\n";
        stubFor(get(urlEqualTo("/cgi-bin/admin/getfile.exe?CISOMODE=1&CISOFILE=/gilmer/index/description/export.xml"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/octet-stream")
                        .withBody(modifiedBody)));

        String[] argsRerun = exportArgs(projPath);
        executeExpectFailure(argsRerun);
        assertOutputContains("Export has already completed, must force restart to overwrite");

        MigrationProject project = MigrationProjectFactory.loadMigrationProject(projPath);

        // Previous export should still be present
        assertExportFilesPresent(project, "export_1.xml");
        assertEquals(originalExportBody, FileUtils.readFileToString(
                project.getExportPath().resolve("export_1.xml").toFile(), StandardCharsets.UTF_8));

        // Retry with force restart
        String[] argsRestart = exportArgs(projPath, "--force");
        executeExpectSuccess(argsRestart);

        assertExportFilesPresent(project, "export_1.xml");
        // Contents of file should match new contents
        assertEquals(modifiedBody, FileUtils.readFileToString(
                project.getExportPath().resolve("export_1.xml").toFile(), StandardCharsets.UTF_8));
    }

    private void assertExportFilesPresent(MigrationProject project, String... expectedNames) throws IOException {
        List<String> exportNames = Files.list(project.getExportPath())
                .map(p -> p.getFileName().toString())
                .filter(p -> p.startsWith("export"))
                .collect(Collectors.toList());
        assertTrue("Expected names: " + String.join(", ", expectedNames
            + "\nBut found names: " + exportNames), exportNames.containsAll(Arrays.asList(expectedNames)));
        assertEquals(expectedNames.length, exportNames.size());
    }

    private Path createProject() throws Exception {
        MigrationProject project = MigrationProjectFactory.createMigrationProject(
                baseDir, COLLECTION_ID, null, USERNAME);
        CdmFieldInfo fieldInfo = new CdmFieldInfo();
        CdmFieldEntry fieldEntry = new CdmFieldEntry();
        fieldEntry.setNickName("title");
        fieldEntry.setExportAs("titla");
        fieldEntry.setDescription("Title");
        fieldInfo.getFields().add(fieldEntry);
        CdmFieldEntry fieldEntry2 = new CdmFieldEntry();
        fieldEntry2.setNickName("title2");
        fieldEntry2.setExportAs("titlb");
        fieldEntry2.setDescription("Another Title");
        fieldInfo.getFields().add(fieldEntry2);
        fieldService.persistFieldsToProject(project, fieldInfo);
        return project.getProjectPath();
    }

}
