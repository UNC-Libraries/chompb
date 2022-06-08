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

import static edu.unc.lib.boxc.migration.cdm.services.ProjectPropertiesService.HOOK_ID;
import static edu.unc.lib.boxc.migration.cdm.services.ProjectPropertiesService.COLLECTION_NUMBER;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;

/**
 * @author krwong
 */
public class ProjectPropertiesServiceTest {
    private static final String PROJECT_NAME = "gilmer";

    @Rule
    public final TemporaryFolder tmpFolder = new TemporaryFolder();

    private MigrationProject project;
    private ProjectPropertiesService service;

    @Before
    public void setup() throws Exception {
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder.getRoot().toPath(), PROJECT_NAME, null, "user", CdmEnvironmentHelper.DEFAULT_ENV_ID);
        service = new ProjectPropertiesService();
        service.setProject(project);
    }

    @Test
    public void listProjectPropertiesTest() throws Exception {
        Map<String, String> projectProperties = service.getProjectProperties();

        assertTrue(projectProperties.containsKey(HOOK_ID));
        assertTrue(projectProperties.containsValue(null));
        assertTrue(projectProperties.containsKey(COLLECTION_NUMBER));
    }

    @Test
    public void setProjectPropertiesTest() throws Exception {
        service.setProperty(HOOK_ID, "000dd");
        service.setProperty(COLLECTION_NUMBER, "000dd");

        assertEquals("000dd", project.getProjectProperties().getHookId());
        assertEquals("000dd", project.getProjectProperties().getCollectionNumber());
    }

    @Test
    public void setProjectPropertiesInvalidFieldTest() throws Exception {
        try {
            service.setProperty("ookId", "000dd");
            fail();
        } catch (MigrationException e) {
            assertTrue(e.getMessage().contains("Invalid field/value input"));
        }
    }

    @Test
    public void setProjectPropertyNullValueTest() throws Exception {
        try {
            service.setProperty(HOOK_ID, null);
            fail();
        } catch (MigrationException e) {
            assertTrue(e.getMessage().contains("Invalid field/value input"));
        }
    }

    @Test
    public void unsetProjectPropertiesTest() throws Exception {
        List<String> unsetFields = new ArrayList<>();
        unsetFields.add(HOOK_ID);
        unsetFields.add(COLLECTION_NUMBER);

        service.unsetProperties(unsetFields);

        assertNull(project.getProjectProperties().getHookId());
        assertNull(project.getProjectProperties().getCollectionNumber());
    }

    @Test
    public void unsetProjectPropertiesFailTest() throws Exception {
        List<String> unsetFields = new ArrayList<>();
        unsetFields.add("ookId");

        try {
            service.unsetProperties(unsetFields);
            fail();
        } catch (MigrationException e) {
            assertTrue(e.getMessage().contains("Invalid project property(ies)"));
        }
    }
}
