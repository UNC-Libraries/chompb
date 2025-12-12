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
        verifier.setChompbConfig(parentCommand.getChompbConfig());
    }

    @Override
    public Integer call() throws Exception {
        try {
            long start = System.nanoTime();
            init();
            var outcome = verifier.verify();
            outputLogger.info("");
            outputLogger.info("Completed verification in {}s", (System.nanoTime() - start) / 1e9);
            outputLogger.info("Checked {} out of {} objects in the report",
                    outcome.verifiedCount, outcome.totalRecords);
            if (outcome.hasErrors()) {
                outputLogger.info("Boxc URL Errors encountered for {} objects, see report for details:", outcome.urlErrorCount);
                outputLogger.info("Parent Collection Errors encountered for {} objects:", outcome.parentCollErrorCount);
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
