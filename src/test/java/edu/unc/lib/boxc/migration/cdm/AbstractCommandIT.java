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
import static org.slf4j.LoggerFactory.getLogger;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Path;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;

import picocli.CommandLine;

/**
 * @author bbpennel
 */
public class AbstractCommandIT {
    private static final Logger log = getLogger(AbstractCommandIT.class);
    protected final static String USERNAME = "theuser";
    private final String initialUser = System.getProperty("user.name");

    @Rule
    public final TemporaryFolder tmpFolder = new TemporaryFolder();
    protected Path baseDir;

    protected final PrintStream originalOut = System.out;
    protected final ByteArrayOutputStream out = new ByteArrayOutputStream();
    protected String output;
    protected CommandLine migrationCommand;

    @After
    public void resetOut() {
        System.setOut(originalOut);
        System.setProperty("user.name", initialUser);
    }

    @Before
    public void baseSetUp() throws Exception {
        System.setProperty("user.name", USERNAME);

        tmpFolder.create();
        baseDir = tmpFolder.getRoot().toPath();

        out.reset();
        System.setOut(new PrintStream(out));
        output = null;

        migrationCommand = new CommandLine(new CLIMain());
    }

    protected void executeExpectSuccess(String[] args) {
        int result = migrationCommand.execute(args);
        output = out.toString();
        if (result != 0) {
            System.setOut(originalOut);
            log.error(output);
            fail("Expected command to result in success: " + String.join(" ", args));
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
