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

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.OUTPUT_LOGGER;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.concurrent.Callable;

import org.slf4j.Logger;

import edu.unc.lib.boxc.migration.cdm.util.BannerUtility;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * Main class for the CLI utils
 *
 * @author bbpennel
 *
 */
@Command(subcommands = {
    })
public class CLIMain implements Callable<Integer> {
    private static final Logger output = getLogger(OUTPUT_LOGGER);

    protected CLIMain() {
    }

    @Override
    public Integer call() throws Exception {
        output.info(BannerUtility.getBanner());
        return 0;
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new CLIMain()).execute(args);
        System.exit(exitCode);
    }

}
