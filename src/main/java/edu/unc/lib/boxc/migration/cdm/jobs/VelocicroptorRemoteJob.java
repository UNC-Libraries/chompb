package edu.unc.lib.boxc.migration.cdm.jobs;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.ProcessSourceFilesOptions;
import edu.unc.lib.boxc.migration.cdm.services.SourceFilesToRemoteService;
import edu.unc.lib.boxc.migration.cdm.util.SshClientService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * Job which prepares and executes a remote velocicroptor job to crop color bars from images
 * @author bbpennel
 */
public class VelocicroptorRemoteJob {
    protected static final String RESULTS_REL_PATH = "processing/results/velocicroptor";
    private static final String JOB_ID_PATTERN_FORMAT = "ddMMyyyyHHmmssSSS";
    private static final DateTimeFormatter JOB_ID_FORMATTER = DateTimeFormatter.ofPattern(JOB_ID_PATTERN_FORMAT)
            .withZone(ZoneId.systemDefault());
    private static final String JOB_FILENAME = "velocicroptor_job.sh";

    private SshClientService sshClientService;
    private MigrationProject project;
    private SourceFilesToRemoteService sourceFilesToRemoteService;
    private Path remoteProjectsPath;
    private Path remoteJobScriptsPath;
    private String adminEmail;
    private String outputServer;
    private Path outputPath;

    /**
     * Perform the velocicroptor job on the source files for the project
     * @param options options for the job
     * @return id of the job that was performed
     */
    public String run(ProcessSourceFilesOptions options) {
        // Generate job id
        var startInstant = Instant.now();
        var jobId = JOB_ID_FORMATTER.format(startInstant);

        // Create local results directory
        var resultsPath = project.getProjectPath().resolve(RESULTS_REL_PATH);
        try {
            Files.createDirectories(resultsPath);

            var remoteJobPath = remoteProjectsPath.resolve(project.getProjectName() + "/velocicroptor/" + jobId);
            // Transfer source files to remote server
            var remoteSourcesPath = remoteJobPath.resolve("source_files");
            sourceFilesToRemoteService.transferFiles(remoteSourcesPath);

            // Create remote job execution config file
            ObjectMapper mapper = new ObjectMapper();
            var config = createJobConfig(options, startInstant, jobId);
            String configJson = mapper.writeValueAsString(config);

            // Trigger remote job, passing config as argument
            var scriptPath = remoteJobScriptsPath.resolve(JOB_FILENAME).toAbsolutePath();
            sshClientService.executeRemoteCommand("sbatch " + scriptPath + " '" + configJson + "'");
        } catch (IOException e) {
            throw new MigrationException(e);
        }
        return jobId;
    }

    private Map<String, String> createJobConfig(ProcessSourceFilesOptions options, Instant startInstant, String jobId) {
        Map<String, String> config = new HashMap<>();
        config.put("job_id", jobId);
        config.put("job_name", options.getActionName());
        config.put("chompb_proj_name", project.getProjectName());
        config.put("admin_address", adminEmail);
        // User that initiated the job
        config.put("username", options.getUsername());
        config.put("email_address", options.getEmailAddress());
        config.put("start_time", startInstant.toString());
        // Details about where the job should send the results once it has completed
        config.put("output_path", outputPath.toString());
        config.put("output_server", outputServer);
        return config;
    }

    public void setSshClientService(SshClientService sshClientService) {
        this.sshClientService = sshClientService;
    }

    public void setProject(MigrationProject project) {
        this.project = project;
    }

    public void setSourceFilesToRemoteService(SourceFilesToRemoteService sourceFilesToRemoteService) {
        this.sourceFilesToRemoteService = sourceFilesToRemoteService;
    }

    public void setRemoteProjectsPath(Path remoteProjectsPath) {
        this.remoteProjectsPath = remoteProjectsPath;
    }

    public void setRemoteJobScriptsPath(Path remoteJobScriptsPath) {
        this.remoteJobScriptsPath = remoteJobScriptsPath;
    }

    public void setAdminEmail(String adminEmail) {
        this.adminEmail = adminEmail;
    }

    public void setOutputServer(String outputServer) {
        this.outputServer = outputServer;
    }

    public void setOutputPath(Path outputPath) {
        this.outputPath = outputPath;
    }
}
