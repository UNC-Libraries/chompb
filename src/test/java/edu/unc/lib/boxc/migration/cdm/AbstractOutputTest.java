package edu.unc.lib.boxc.migration.cdm;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.awaitility.Awaitility.await;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;

/**
 * @author bbpennel
 */
public class AbstractOutputTest {
    @TempDir
    public Path tmpFolder;
    protected Path baseDir;

    protected final PrintStream originalOut = System.out;
    protected final ByteArrayOutputStream out = new ByteArrayOutputStream();
    protected String output;

    protected final PrintStream originalErr = System.err;
    protected ByteArrayOutputStream err;

    @AfterEach
    public void resetOut() {
        System.setOut(originalOut);
    }

    @BeforeEach
    public void setupOutput() throws Exception {
        baseDir = tmpFolder;

        out.reset();
        System.setOut(new PrintStream(out));
        output = null;
    }

    /**
     * Setup capture of error output to a stream.
     * Note: This should be called before the command is executed,
     * and resetError should be called before making any assertions.
     */
    protected void setupError() {
        err = new ByteArrayOutputStream();
        err.reset();
        System.setErr(new PrintStream(err));
    }

    protected void resetError() {
        System.setErr(originalErr);
    }

    protected void resetOutput() {
        out.reset();
        if (err != null) {
            err.reset();
        }
        output = null;
    }

    protected void assertOutputDoesNotContain(String expected) {
        assertFalse(getOutput().contains(expected), "Expected output not to contain:\n" + expected
                        + "\nBut was:\n" + getOutput());
    }

    protected void assertOutputContains(String expected) {
        assertTrue(getOutput().contains(expected), "Expected output to contain:\n" + expected
                        + "\nBut was:\n" + getOutput());
    }

    /**
     * @param expected Value which the output should match, may be a regex
     */
    protected void assertOutputMatches(String expected) {
        Matcher matcher = Pattern.compile(expected, Pattern.DOTALL).matcher(getOutput());
        assertTrue(matcher.matches(), "Expected output to match:\n" + expected
                        + "\nBut was:\n" + getOutput());
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
        assertFalse(matcher.matches(), "Expected output not to match:\n" + expected
                        + "\nBut was:\n" + getOutput());
    }

    protected String getOutput() {
        if (output == null) {
            output = out.toString();
        }
        if (err != null) {
            output += err.toString();
        }
        return output;
    }
}
