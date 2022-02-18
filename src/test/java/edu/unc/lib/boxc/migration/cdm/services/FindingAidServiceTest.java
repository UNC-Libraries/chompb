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

import static edu.unc.lib.boxc.migration.cdm.services.FindingAidService.CONTRI_FIELD;
import static edu.unc.lib.boxc.migration.cdm.services.FindingAidService.DESCRI_FIELD;
import static edu.unc.lib.boxc.migration.cdm.services.FindingAidService.HOOK_ID;
import static edu.unc.lib.boxc.migration.cdm.services.FindingAidService.COLLECTION_NUMBER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Paths;

public class FindingAidServiceTest {
    private static final String PROJECT_NAME = "gilmer";

    @Rule
    public final TemporaryFolder tmpFolder = new TemporaryFolder();

    private MigrationProject project;
    private CdmFieldService fieldService;
    private FindingAidService service;

    @Before
    public void setup() throws Exception {
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder.getRoot().toPath(), PROJECT_NAME, null, "user");

        fieldService = new CdmFieldService();
        service = new FindingAidService();
        service.setCdmFieldService(fieldService);
        service.setProject(project);
    }

    @Test
    public void recordFindingAidTest() throws Exception {
        Files.copy(Paths.get("src/test/resources/findingaid_fields.csv"), project.getFieldsPath());
        service.recordFindingAid();

        assertEquals(CONTRI_FIELD, project.getProjectProperties().getHookId());
        assertEquals(DESCRI_FIELD, project.getProjectProperties().getCollectionNumber());
    }

    @Test
    public void recordFindingAidFailTest() throws Exception {
        try {
            service.recordFindingAid();
            fail();
        } catch (MigrationException e) {
            //Expected
        }
    }

    @Test
    public void noFindingAidToRecordTest() throws Exception {
        populateFieldInfo();
        service.recordFindingAid();

        assertNull(project.getProjectProperties().getHookId());
        assertNull(project.getProjectProperties().getCollectionNumber());
    }

    private void populateFieldInfo() throws Exception {
        Files.copy(Paths.get("src/test/resources/gilmer_fields.csv"), project.getFieldsPath());
    }
}
