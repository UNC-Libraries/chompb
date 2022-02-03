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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.ArrayList;

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
                tmpFolder.getRoot().toPath(), PROJECT_NAME, null, "user");
        service = new ProjectPropertiesService();
        service.setProject(project);
    }

    @Test
    public void listProjectPropertiesTest() throws Exception {
        ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outContent));

        service.getProjectProperties();

        assertEquals("hookId: null\ncollectionNumber: null\n", outContent.toString());
    }

    @Test
    public void setProjectPropertiesTest() throws Exception {
        service.setProperties("hookId", "000dd");
        service.setProperties("collectionNumber", "000dd");

        assertEquals("000dd", project.getProjectProperties().getHookId());
        assertEquals("000dd", project.getProjectProperties().getCollectionNumber());
    }

    @Test
    public void setProjectPropertiesInvalidFieldTest() throws Exception {
        try {
            service.setProperties("ookId", "000dd");
            fail();
        } catch (MigrationException e) {
            assertTrue(e.getMessage().contains("Invalid field/value input"));
        }
    }

    @Test
    public void setProjectPropertiesNullValueTest() throws Exception {
        try {
            service.setProperties("hookId",null);
            fail();
        } catch (MigrationException e) {
            assertTrue(e.getMessage().contains("Invalid field/value input"));
        }
    }

    @Test
    public void unsetProjectPropertiesTest() throws Exception {
        List<String> unsetFields = new ArrayList<>();
        unsetFields.add("hookId");
        unsetFields.add("collectionNumber");

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
