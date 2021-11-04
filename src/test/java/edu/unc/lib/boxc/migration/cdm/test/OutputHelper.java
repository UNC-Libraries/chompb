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
