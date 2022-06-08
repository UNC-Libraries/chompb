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

import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo.CdmFieldEntry;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.CdmFieldService;
import edu.unc.lib.boxc.migration.cdm.services.CdmFileRetrievalService;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.TestSshServer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

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
        setupChompbConfig();
    }

    @After
    public void cleanup() throws Exception {
        testSshServer.stopServer();
    }

    private String[] exportArgs(Path projPath, String... extras) {
        String[] defaultArgs = new String[] {
                "-w", projPath.toString(),
                "--env-config", chompbConfigPath,
                "export",
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
        assertDescAllFilePresent(project, "/descriptions/gilmer/index/description/desc.all");
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
        Path projPath = createProject("bad_colletion");

        String[] args = exportArgs(projPath);
        executeExpectFailure(args);

        MigrationProject project = MigrationProjectFactory.loadMigrationProject(projPath);

        assertFalse("Export file should not be created", Files.exists(CdmFileRetrievalService.getDescAllPath(project)));
        assertOutputContains("Failed to download desc.all file");
    }

    @Test
    public void rerunCompletedExportTest() throws Exception {
        Path projPath = createProject();

        String[] args = exportArgs(projPath);
        executeExpectSuccess(args);

        String[] argsRerun = exportArgs(projPath);
        executeExpectFailure(argsRerun);
        assertOutputContains("Export has already completed, must force restart to overwrite");

        MigrationProject project = MigrationProjectFactory.loadMigrationProject(projPath);

        // Previous export should still be present
        assertDescAllFilePresent(project, "/descriptions/gilmer/index/description/desc.all");

        // Remove file so we can see that it gets repopulated
        Files.delete(CdmFileRetrievalService.getDescAllPath(project));

        // Retry with force restart
        String[] argsRestart = exportArgs(projPath, "--force");
        executeExpectSuccess(argsRestart);

        // Contents of file should match new contents
        assertDescAllFilePresent(project, "/descriptions/gilmer/index/description/desc.all");
    }

    @Test
    public void exportValidProjectWithCompoundsTest() throws Exception {
        Path projPath = createProject("mini_keepsakes");

        String[] args = exportArgs(projPath);
        executeExpectSuccess(args);

        MigrationProject project = MigrationProjectFactory.loadMigrationProject(projPath);

        assertTrue("Export folder not created", Files.exists(project.getExportPath()));
        assertDescAllFilePresent(project, "/descriptions/mini_keepsakes/index/description/desc.all");

        assertCpdFilePresent(project, "617.cpd", "/descriptions/mini_keepsakes/image/617.cpd");
        assertCpdFilePresent(project, "620.cpd", "/descriptions/mini_keepsakes/image/620.cpd");
    }

    private void assertDescAllFilePresent(MigrationProject project, String expectedContentPath) throws Exception {
        assertEquals(IOUtils.toString(getClass().getResourceAsStream(expectedContentPath), StandardCharsets.UTF_8),
                FileUtils.readFileToString(CdmFileRetrievalService.getDescAllPath(project).toFile(), StandardCharsets.UTF_8));
    }

    private void assertCpdFilePresent(MigrationProject project, String exportedFilename, String expectedContentPath) throws Exception {
        assertEquals(IOUtils.toString(getClass().getResourceAsStream(expectedContentPath), StandardCharsets.UTF_8),
                FileUtils.readFileToString(CdmFileRetrievalService.getExportedCpdsPath(project).resolve(exportedFilename).toFile(), StandardCharsets.UTF_8));
    }

    private Path createProject() throws Exception {
        return createProject(COLLECTION_ID);
    }

    private Path createProject(String collId) throws Exception {
        MigrationProject project = MigrationProjectFactory.createMigrationProject(
                baseDir, collId, null, USERNAME, CdmEnvironmentHelper.DEFAULT_ENV_ID);
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
