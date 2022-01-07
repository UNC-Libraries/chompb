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

import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.services.RedirectMappingIndexService;
import edu.unc.lib.boxc.migration.cdm.test.SipServiceHelper;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.Assert.assertTrue;

/**
 * @author snluong
 */
public class IndexRedirectCommandIT  extends AbstractCommandIT {
    private static final String PROJECT_NAME = "proj";
    private static final String USERNAME = "migr_user";
    private static final String DEST_UUID = "7a33f5e6-f0ca-461c-8df0-c76c62198b17";
    private MigrationProject project;
    private SipServiceHelper testHelper;

    private RedirectMappingIndexService indexService;

    @Before
    public void setup() throws Exception {
        project = MigrationProjectFactory.createMigrationProject(
                baseDir, PROJECT_NAME, null, USERNAME);
        testHelper = new SipServiceHelper(project, tmpFolder.newFolder().toPath());
        Files.createDirectories(project.getExportPath());
        testHelper.initializeDefaultProjectState(DEST_UUID);
    }

    @Test
    public void indexRedirectsFailsIfProjectNotSubmitted() {
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "index_redirects"};
        executeExpectFailure(args);

        assertOutputContains("Must submit the collection prior to indexing");
    }

    @Test
    public void indexRedirectsFailsIfProjectNotSubmitted() {
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "index_redirects"};
        executeExpectFailure(args);

        assertOutputContains("Must submit the collection prior to indexing");
    }
}
