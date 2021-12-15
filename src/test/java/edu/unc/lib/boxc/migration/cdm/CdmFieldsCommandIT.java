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

import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo.CdmFieldEntry;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.CdmFieldService;
import edu.unc.lib.boxc.migration.cdm.services.FieldAssessmentTemplateService;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;

/**
 * @author bbpennel
 */
public class CdmFieldsCommandIT extends AbstractCommandIT {

    private CdmFieldService fieldService;
    private FieldAssessmentTemplateService templateService;

    @Rule
    public final TemporaryFolder tmpFolder = new TemporaryFolder();

    @Before
    public void setUp() throws Exception {
        fieldService = new CdmFieldService();
        templateService = new FieldAssessmentTemplateService();
        templateService.setCdmFieldService(fieldService);
    }

    @After
    public void cleanup() {
        System.setOut(originalOut);
    }

    @Test
    public void validateValidFieldsConfigTest() throws Exception {
        Path projPath = baseDir.resolve("proj");
        MigrationProject project = MigrationProjectFactory.createMigrationProject(projPath, null, null, USERNAME);
        CdmFieldInfo fieldInfo = new CdmFieldInfo();
        CdmFieldEntry fieldEntry = new CdmFieldEntry();
        fieldEntry.setNickName("title");
        fieldEntry.setExportAs("titla");
        fieldEntry.setDescription("Title");
        fieldInfo.getFields().add(fieldEntry);
        fieldService.persistFieldsToProject(project, fieldInfo);

        String[] cmdArgs = new String[] {
                "-w", projPath.toString(),
                "fields", "validate"};
        executeExpectSuccess(cmdArgs);
    }

    @Test
    public void validateInvalidFieldsConfigTest() throws Exception {
        Path projPath = baseDir.resolve("proj");
        MigrationProject project = MigrationProjectFactory.createMigrationProject(projPath, null, null, USERNAME);
        CdmFieldInfo fieldInfo = new CdmFieldInfo();
        CdmFieldEntry fieldEntry = new CdmFieldEntry();
        fieldEntry.setNickName("title");
        fieldEntry.setExportAs("titla");
        fieldEntry.setDescription("Title");
        fieldInfo.getFields().add(fieldEntry);
        CdmFieldEntry fieldEntry2 = new CdmFieldEntry();
        fieldEntry2.setNickName("title2");
        fieldEntry2.setExportAs("titla");
        fieldEntry2.setDescription("Another Title");
        fieldInfo.getFields().add(fieldEntry2);
        fieldService.persistFieldsToProject(project, fieldInfo);

        String[] cmdArgs = new String[] {
                "-w", projPath.toString(),
                "fields", "validate"};
        executeExpectFailure(cmdArgs);

        assertOutputContains("Duplicate export_as value 'titla'");
    }

    @Test
    public void generateReportTest() throws Exception {
        tmpFolder.create();
        MigrationProject project = MigrationProjectFactory.createMigrationProject(
                tmpFolder.getRoot().toPath(), "gilmer", null, USERNAME);
        Files.copy(Paths.get("src/test/resources/gilmer_fields.csv"), project.getFieldsPath());
        templateService.generate(project);

        Path projPath = project.getProjectPath();
        Path newPath = projPath.resolve("field_assessment_gilmer.xlsx");

        assertTrue(Files.exists(newPath));
    }
}
