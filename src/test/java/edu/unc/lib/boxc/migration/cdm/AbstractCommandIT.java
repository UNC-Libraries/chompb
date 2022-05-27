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
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.SipServiceHelper;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import picocli.CommandLine;

import java.io.IOException;

import static org.junit.Assert.fail;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author bbpennel
 */
public class AbstractCommandIT extends AbstractOutputTest {
    private static final Logger log = getLogger(AbstractCommandIT.class);
    protected final static String COLLECTION_ID = "my_coll";
    protected final static String PROJECT_ID = "my_proj";
    protected final static String USERNAME = "theuser";
    private final String initialUser = System.getProperty("user.name");

    protected CommandLine migrationCommand;
    protected SipServiceHelper testHelper;

    protected MigrationProject project;

    @After
    public void resetProps() {
        System.setProperty("user.name", initialUser);
    }

    @Before
    public void baseSetUp() throws Exception {
        System.setProperty("user.name", USERNAME);
        migrationCommand = new CommandLine(new CLIMain());
        tmpFolder.create();
    }

    protected void initTestHelper() throws IOException {
        testHelper = new SipServiceHelper(project, tmpFolder.newFolder().toPath());;
    }

    protected void initProject() throws IOException {
        project = MigrationProjectFactory.createMigrationProject(
                baseDir, PROJECT_ID, COLLECTION_ID, USERNAME, CdmEnvironmentHelper.getTestEnv());
    }

    protected void initProjectAndHelper() throws IOException {
        initProject();
        initTestHelper();
    }

    protected void executeExpectSuccess(String[] args) {
        int result = migrationCommand.execute(args);
        output = out.toString();
        if (result != 0) {
            System.setOut(originalOut);
            // Can't see the output from the command without this
            System.out.println(output);
            fail("Expected command to result in success: " + String.join(" ", args) + "\nWith output:\n" + output);
        }
    }

    protected void executeExpectFailure(String[] args) {
        int result = migrationCommand.execute(args);
        output = out.toString();
        if (result == 0) {
            System.setOut(originalOut);
            log.error(output);
            fail("Expected command to result in failure: " + String.join(" ", args));
        }
    }
}
