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
package edu.unc.lib.boxc.migration.cdm.services;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.ss.usermodel.Sheet;

import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;

/**
 * @author krwong
 */
public class FieldAssessmentTemplateServiceTest {
    private static final String PROJECT_NAME = "gilmer";
    @Rule
    public final TemporaryFolder tmpFolder = new TemporaryFolder();

    private MigrationProject project;
    private CdmFieldService fieldService;
    private FieldAssessmentTemplateService service;

    @Before
    public void setup() throws Exception {
        tmpFolder.create();
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder.getRoot().toPath(), PROJECT_NAME, null, "user", CdmEnvironmentHelper.DEFAULT_ENV);
        fieldService = new CdmFieldService();

        service = new FieldAssessmentTemplateService();
        service.setCdmFieldService(fieldService);
    }

    @Test
    public void allExpectedCellsPopulatedTest() throws Exception {
        populateFieldInfo();
        service.generate(project);

        Path projPath = project.getProjectPath();
        InputStream inputStream = Files.newInputStream(projPath.resolve("field_assessment_gilmer.xlsx"));
        XSSFWorkbook workbook = new XSSFWorkbook(inputStream);
        Sheet sheet = workbook.getSheetAt(0);

        assertEquals(60, sheet.getLastRowNum());
        assertEquals(16, sheet.getRow(0).getPhysicalNumberOfCells());
        assertEquals(12, sheet.getRow(1).getPhysicalNumberOfCells());
        assertEquals(12, sheet.getRow(60).getPhysicalNumberOfCells());
        assertNull(sheet.getRow(1).getCell(2));
        assertNull(sheet.getRow(1).getCell(8));
        assertNull(sheet.getRow(1).getCell(9));
        assertNull(sheet.getRow(1).getCell(10));
        assertNotEquals("Search by Decade", workbook.getSheetAt(0).getRow(7).getCell(0));
        assertNotEquals("search", workbook.getSheetAt(0).getRow(7).getCell(1));
    }

    @Test
    public void regenerateTemplateTest() throws Exception {
        populateFieldInfo();
        service.generate(project);
        service.generate(project);

        Path projPath = project.getProjectPath();
        Path newPath = projPath.resolve("field_assessment_gilmer.xlsx");

        assertTrue(Files.exists(newPath));
    }

    @Test
    public void invalidFieldsTest() throws Exception {
        try {
            service.generate(project);
            fail();
        } catch (InvalidProjectStateException e) {
            assertTrue(e.getMessage().contains("CDM fields file is missing"));
        }
    }

    private void populateFieldInfo() throws Exception {
        Files.copy(Paths.get("src/test/resources/gilmer_fields.csv"), project.getFieldsPath());
    }
}
