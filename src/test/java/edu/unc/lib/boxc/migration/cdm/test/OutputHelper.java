package edu.unc.lib.boxc.migration.cdm.test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

/**
 * @author bbpennel
 */
public class OutputHelper {

    @FunctionalInterface
    public interface CheckedRunnable {
        void run() throws Exception;
    }

    /**
     * Capture the output of the provided code block
     * @param runnable
     * @return
     */
    public static String captureOutput(CheckedRunnable runnable) {
        PrintStream originalOut = System.out;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        System.setOut(new PrintStream(out));

        try {
            runnable.run();
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new RuntimeException(e);
        } finally {
            System.setOut(originalOut);
        }
        return out.toString();
    }
}
