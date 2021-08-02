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

import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Test;

import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;

/**
 * Test which runs a single collection through a full set of migration steps
 * @author bbpennel
 */
public class CompleteMigrationIT extends AbstractCommandIT {
    private final static String COLLECTION_ID = "my_coll";
    private final static String DEST_UUID = "3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e";

    private MigrationProject project;

    @Before
    public void setup() throws Exception {
        project = MigrationProjectFactory.createMigrationProject(
                baseDir, COLLECTION_ID, null, USERNAME);
    }

    @Test
    public void generateNotIndexedTest() throws Exception {
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "sips", "generate" };
        executeExpectFailure(args);

        assertOutputContains("Project must be indexed");
        fail();
    }
}
