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
import edu.unc.lib.boxc.migration.cdm.services.CdmFileRetrievalService;
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

    private TestSshServer testSshServer;

    private CdmFieldService fieldService;

    @Before
    public void setUp() throws Exception {
        fieldService = new CdmFieldService();
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
                "-P", "42222",
                "-p", PASSWORD};
        return ArrayUtils.addAll(defaultArgs, extras);
    }

    @Test
    public void exportValidProjectTest() throws Exception {
        Path projPath = createProject();

        String[] args = exportArgs(projPath);
        executeExpectSuccess(args);

        MigrationProject project = MigrationProjectFactory.loadMigrationProject(projPath);

        assertTrue("Export folder not created", Files.exists(project.getExportPath()));
        assertEquals(IOUtils.toString(getClass().getResourceAsStream("/descriptions/gilmer/index/description/desc.all"), StandardCharsets.UTF_8),
                FileUtils.readFileToString(CdmFileRetrievalService.getDescAllPath(project).toFile(), StandardCharsets.UTF_8));
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
                "export" };
        executeExpectFailure(args);

        MigrationProject project = MigrationProjectFactory.loadMigrationProject(projPath);
        assertFalse("Description folder should not be created", Files.exists(project.getExportPath()));
        assertOutputContains("Must provided a CDM password");
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
    public void rerunCompletedExportTest() throws Exception {
        Path projPath = createProject();

        String[] args = exportArgs(projPath);
        executeExpectSuccess(args);

        // Change response for export so it will be clear if a new export occurs

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
