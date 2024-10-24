package edu.unc.lib.boxc.migration.cdm;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.jobs.VelocicroptorRemoteJob;
import edu.unc.lib.boxc.migration.cdm.model.BxcEnvironment;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.ProcessSourceFilesOptions;
import edu.unc.lib.boxc.migration.cdm.services.CdmIndexService;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.services.SourceFileService;
import edu.unc.lib.boxc.migration.cdm.services.SourceFilesToRemoteService;
import edu.unc.lib.boxc.migration.cdm.util.SshClientService;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.Callable;

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author bbpennel
 */
@CommandLine.Command(name = "process_source_files",
        description = {
                "Perform a processing job on the source files mapped in this project."})
public class ProcessSourceFilesCommand implements Callable<Integer> {
    private static final Logger log = getLogger(ProcessSourceFilesCommand.class);
    private static final String DEFAULT_EMAIL_DOMAIN = "@ad.unc.edu";

    @CommandLine.ParentCommand
    private CLIMain parentCommand;
    private VelocicroptorRemoteJob velocicroptorRemoteJob;

    private MigrationProject project;
    private BxcEnvironment boxcEnv;

    @CommandLine.Mixin
    private ProcessSourceFilesOptions options;

    @Override
    public Integer call() throws Exception {
        long start = System.nanoTime();
        try {
            validateActionName(options.getActionName());
            loadProjectEnvironment();
            setDefaultOptions();
            initialize();
            velocicroptorRemoteJob.run(options);
            outputLogger.info("Completed {} job to process source files for {} in {}s",
                    options.getActionName(), project.getProjectName(), (System.nanoTime() - start) / 1e9);
            return 0;
        } catch (MigrationException | IllegalArgumentException e) {
            outputLogger.info("Source file processing command failed: {}", e.getMessage());
            log.warn("Source file processing command failed", e);
            return 1;
        } catch (Exception e) {
            log.error("Source file processing command failed", e);
            outputLogger.info("Source file processing command failed: {}", e.getMessage(), e);
            return 1;
        }
    }

    private void validateActionName(String actionName) {
        if (!actionName.equals(VelocicroptorRemoteJob.JOB_NAME)) {
            throw new IllegalArgumentException("Invalid action name provided: " + actionName);
        }
    }

    private void setDefaultOptions() {
        if (options.getEmailAddress() == null) {
            options.setEmailAddress(options.getUsername() + DEFAULT_EMAIL_DOMAIN);
        }
    }

    private void loadProjectEnvironment() throws IOException {
        Path currentPath = parentCommand.getWorkingDirectory();
        project = MigrationProjectFactory.loadMigrationProject(currentPath);
        var config = parentCommand.getChompbConfig();
        boxcEnv = config.getBxcEnvironments().get(project.getProjectProperties().getBxcEnvironmentId());
    }

    private void initialize() throws IOException {
        // Separate service for executing scripts on the remote server
        var sshClientScriptService = new SshClientService();
        sshClientScriptService.setSshHost(boxcEnv.getBoxctronScriptHost());
        sshClientScriptService.setSshPort(boxcEnv.getBoxctronPort());
        sshClientScriptService.setSshKeyPath(options.getSshKeyPath());
        sshClientScriptService.setSshUsername(options.getUsername());
        sshClientScriptService.initialize();
        // Separate service for transferring files to the remote server
        var sshClientTransferService = new SshClientService();
        sshClientTransferService.setSshHost(boxcEnv.getBoxctronTransferHost());
        sshClientTransferService.setSshPort(boxcEnv.getBoxctronPort());
        sshClientTransferService.setSshKeyPath(options.getSshKeyPath());
        sshClientTransferService.setSshUsername(options.getUsername());
        sshClientTransferService.initialize();
        var cdmIndexService = new CdmIndexService();
        cdmIndexService.setProject(project);
        var sourceFileService = new SourceFileService();
        sourceFileService.setProject(project);
        sourceFileService.setIndexService(cdmIndexService);
        var sourceFilesToRemoteService = new SourceFilesToRemoteService();
        sourceFilesToRemoteService.setSourceFileService(sourceFileService);
        sourceFilesToRemoteService.setSshClientService(sshClientTransferService);
        velocicroptorRemoteJob = new VelocicroptorRemoteJob();
        velocicroptorRemoteJob.setProject(project);
        velocicroptorRemoteJob.setSshClientService(sshClientScriptService);
        velocicroptorRemoteJob.setOutputServer(boxcEnv.getBoxctronOutputServer());
        velocicroptorRemoteJob.setOutputPath(boxcEnv.getBoxctronOutputBasePath());
        velocicroptorRemoteJob.setRemoteProjectsPath(boxcEnv.getBoxctronRemoteProjectsPath());
        velocicroptorRemoteJob.setAdminEmail(boxcEnv.getBoxctronAdminEmail());
        velocicroptorRemoteJob.setRemoteJobScriptsPath(boxcEnv.getBoxctronRemoteJobScriptsPath());
        velocicroptorRemoteJob.setSourceFilesToRemoteService(sourceFilesToRemoteService);
    }
}
