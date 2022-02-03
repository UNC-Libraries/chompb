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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.services.ProjectPropertiesService;

/**
 * @author krwong
 */
public class ProjectPropertiesCommandIT extends AbstractCommandIT {
    private static final String PROJECT_NAME = "gilmer";

    @Rule
    public final TemporaryFolder tmpFolder = new TemporaryFolder();

    private MigrationProject project;
    private ProjectPropertiesService propertiesService;

    @Before
    public void setup() throws Exception {
        project = MigrationProjectFactory.createMigrationProject(
                baseDir, PROJECT_NAME, null,"user");
        propertiesService = new ProjectPropertiesService();
        propertiesService.setProject(project);
    }

    @Test
    public void listProjectPropertiesTest() throws Exception {
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "config", "list"};
        executeExpectSuccess(args);

        assertOutputContains("hookId: null\ncollectionNumber: null\n");
    }

    @Test
    public void setProjectPropertiesTest() throws Exception {
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "config", "set", "collectionNumber", "000dd"};
        executeExpectSuccess(args);

        assertOutputContains("Project property set");
    }

    @Test
    public void setProjectPropertiesInvalidFieldTest() throws Exception {
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "config", "set", "ookId", "000dd"};
        executeExpectFailure(args);

        assertOutputContains("Invalid field/value input");
    }

    @Test
    public void setProjectPropertiesNoValueTest() throws Exception {
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "config", "set", "hookId"};
        executeExpectFailure(args);
    }

    @Test
    public void unsetOneProjectPropertyTest() throws Exception {
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "config", "unset", "hookId"};
        executeExpectSuccess(args);

        assertOutputContains("Project property(ies) unset");
    }

    @Test
    public void unsetMultipleProjectPropertiesTest() throws Exception {
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "config", "unset", "hookId", "collectionNumber"};
        executeExpectSuccess(args);

        assertOutputContains("Project property(ies) unset");
    }

    @Test
    public void unsetProjectPropertiesFailTest() throws Exception {
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "config", "unset", "hookId", "collection"};
        executeExpectFailure(args);

        assertOutputContains("Invalid project property(ies)");
    }

}
