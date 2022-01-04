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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.awaitility.Awaitility.await;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.awaitility.core.ConditionTimeoutException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;

/**
 * @author bbpennel
 */
public class AbstractOutputTest {
    @Rule
    public final TemporaryFolder tmpFolder = new TemporaryFolder();
    protected Path baseDir;

    protected final PrintStream originalOut = System.out;
    protected final ByteArrayOutputStream out = new ByteArrayOutputStream();
    protected String output;

    @After
    public void resetOut() {
        System.setOut(originalOut);
    }

    @Before
    public void setupOutput() throws Exception {
        tmpFolder.create();
        baseDir = tmpFolder.getRoot().toPath();

        out.reset();
        System.setOut(new PrintStream(out));
        output = null;
    }

    protected void resetOutput() {
        out.reset();
        output = null;
    }

    protected void assertOutputDoesNotContain(String expected) {
        assertFalse("Expected output not to contain:\n" + expected
                + "\nBut was:\n" + getOutput(), getOutput().contains(expected));
    }

    protected void assertOutputContains(String expected) {
        assertTrue("Expected output to contain:\n" + expected
                + "\nBut was:\n" + getOutput(), getOutput().contains(expected));
    }

    /**
     * @param expected Value which the output should match, may be a regex
     */
    protected void assertOutputMatches(String expected) {
        Matcher matcher = Pattern.compile(expected, Pattern.DOTALL).matcher(getOutput());
        assertTrue("Expected output to match:\n" + expected
                + "\nBut was:\n" + getOutput(), matcher.matches());
    }

    /**
     * Wait a short amount of time for the output to match the provided expression
     * @param expected Value which the output should match, may be a regex
     */
    protected void awaitOutputMatches(String expected) {
        Pattern pattern = Pattern.compile(expected, Pattern.DOTALL);
        try {
            await().pollInterval(Duration.ofMillis(25))
                    .atMost(Duration.ofSeconds(2))
                    .until(() -> {
                        return pattern.matcher(out.toString()).matches();
                    });
        } catch (ConditionTimeoutException e) {
            System.err.println("Expected output to match:\n" + expected + "\nBut was:\n" + out.toString());
            throw e;
        }
    }

    /**
     * @param expected Value which the output should not match, may be a regex
     */
    protected void assertOutputNotMatches(String expected) {
        Matcher matcher = Pattern.compile(expected, Pattern.DOTALL).matcher(getOutput());
        assertFalse("Expected output not to match:\n" + expected
                + "\nBut was:\n" + getOutput(), matcher.matches());
    }

    protected String getOutput() {
        if (output == null) {
            output = out.toString();
        }
        return output;
    }
}
