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

import edu.unc.lib.boxc.migration.cdm.services.SipService;
import edu.unc.lib.boxc.migration.cdm.test.RedirectMappingHelper;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * @author snluong
 */
public class IndexRedirectCommandIT  extends AbstractCommandIT {
    private static final String DEST_UUID = "7a33f5e6-f0ca-461c-8df0-c76c62198b17";
    private SipService sipsService;
    private RedirectMappingHelper redirectMappingHelper;
    private Path propertiesPath;

    @Before
    public void setup() throws Exception {
        initProjectAndHelper();

        Files.createDirectories(project.getExportPath());
        sipsService = testHelper.createSipsService();
        redirectMappingHelper = new RedirectMappingHelper(project);
        redirectMappingHelper.createRedirectMappingsTableInDb();
        propertiesPath = redirectMappingHelper.createDbConnectionPropertiesFile(tmpFolder, "sqlite");
    }

    @Test
    public void indexRedirectsFailsIfProjectNotSubmitted() {
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "index_redirects",
                "--db-connection", propertiesPath.toString()};
        executeExpectFailure(args);

        assertOutputContains("Must submit the collection prior to indexing");
    }

    @Test
    public void indexRedirectsSucceedsIfProjectIsSubmitted() throws Exception {
        testHelper.initializeDefaultProjectState(DEST_UUID);
        sipsService.generateSips(redirectMappingHelper.makeOptions());
        testHelper.addSipsSubmitted();
        ProjectPropertiesSerialization.write(project);

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "index_redirects",
                "--db-connection", propertiesPath.toString()};
        executeExpectSuccess(args);
        assertOutputContains("Redirect mapping indexing completed. Yay!");
    }

    @Test
    public void indexRedirectsFailsIfDbConnectionStringNotIncluded() {
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "index_redirects"};
        executeExpectFailure(args);

        assertOutputContains("The DB connection path must be included");
    }
}
