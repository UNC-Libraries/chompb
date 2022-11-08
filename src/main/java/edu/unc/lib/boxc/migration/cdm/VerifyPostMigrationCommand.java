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

import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.services.PostMigrationReportVerifier;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.Callable;

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;

/**
 * @author bbpennel
 */
@CommandLine.Command(name = "verify_migration",
        description = "Verifies objects have been migrated into box-c")
public class VerifyPostMigrationCommand implements Callable<Integer> {
    @CommandLine.ParentCommand
    private CLIMain parentCommand;
    private CloseableHttpClient httpClient;
    private PostMigrationReportVerifier verifier;
    private MigrationProject project;

    public void init() throws IOException {
        httpClient = HttpClients.createMinimal();
        Path currentPath = parentCommand.getWorkingDirectory();
        project = MigrationProjectFactory.loadMigrationProject(currentPath);
        verifier = new PostMigrationReportVerifier();
        verifier.setHttpClient(httpClient);
        verifier.setProject(project);
        verifier.setShowProgress(true);
    }

    @Override
    public Integer call() throws Exception {
        try {
            long start = System.nanoTime();
            init();
            var outcome = verifier.verify();
            outputLogger.info("");
            outputLogger.info("Completed verification in {}", (System.nanoTime() - start) / 1e9);
            outputLogger.info("Checked {} out of {} objects in the report",
                    outcome.verifiedCount, outcome.totalRecords);
            if (outcome.hasErrors()) {
                outputLogger.info("Errors encountered for {} objects, see report for details:", outcome.errorCount);
                outputLogger.info(project.getPostMigrationReportPath().toString());
                return 1;
            }
            outputLogger.info("Success! No problems were found");
            return 0;
        } catch (InvalidProjectStateException e) {
            outputLogger.info(e.getMessage());
        } catch (Exception e) {
            outputLogger.info("Encountered an error while verifying migration", e);
        }
        return 1;
    }
}
