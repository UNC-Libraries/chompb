package edu.unc.lib.boxc.migration.cdm.jobs;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.ProcessSourceFilesOptions;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.services.SourceFilesToRemoteService;
import edu.unc.lib.boxc.migration.cdm.test.BxcEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.util.SshClientService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.openMocks;

/**
 * @author bbpennel
 */
public class VelocicroptorRemoteJobTest {
    private static final String PROJECT_NAME = "proj";
    private static final String ADMIN_EMAIL = "chompb_admin@example.com";
    private static final String USER_EMAIL = "chompb_user@example.com";
    private static final String USERNAME = "chompb_user";
    private static final String OUTPUT_SERVER = "chompb.example.com";
    private VelocicroptorRemoteJob job;
    @Mock
    private SshClientService sshClientService;
    private MigrationProject project;
    @Mock
    private SourceFilesToRemoteService sourceFilesToRemoteService;
    private Path remoteProjectsPath;
    private Path remoteJobScriptsPath;
    private Path projectPath;
    private Path outputPath;
    @Captor
    private ArgumentCaptor<String> remoteArgumentCaptor;

    private AutoCloseable closeable;
    @TempDir
    public Path tmpFolder;

    @BeforeEach
    public void setup() throws IOException {
        closeable = openMocks(this);
        projectPath = tmpFolder.resolve(PROJECT_NAME);
        project = MigrationProjectFactory.createCdmMigrationProject(
                tmpFolder, PROJECT_NAME, null, USERNAME,
                null, BxcEnvironmentHelper.DEFAULT_ENV_ID);
        remoteProjectsPath = tmpFolder.resolve("remote_projects");
        remoteJobScriptsPath = tmpFolder.resolve("scripts");

        outputPath = projectPath.resolve(VelocicroptorRemoteJob.RESULTS_REL_PATH);

        job = new VelocicroptorRemoteJob();
        job.setSshClientService(sshClientService);
        job.setProject(project);
        job.setSourceFilesToRemoteService(sourceFilesToRemoteService);
        job.setRemoteProjectsPath(remoteProjectsPath);
        job.setRemoteJobScriptsPath(remoteJobScriptsPath);
        job.setAdminEmail(ADMIN_EMAIL);
        job.setOutputServer(OUTPUT_SERVER);
        job.setOutputPath(outputPath);
    }

    @AfterEach
    void tearDown() throws Exception {
        closeable.close();
    }

    @Test
    public void runSuccessfulTest() throws Exception {
        var options = new ProcessSourceFilesOptions();
        options.setActionName("velocicroptor");
        options.setUsername(USERNAME);
        options.setEmailAddress(USER_EMAIL);

        var jobId = job.run(options);

        assertTrue(Files.isDirectory(outputPath));

        var remoteProjectPath = remoteProjectsPath.resolve(project.getProjectName() + "/velocicroptor/" + jobId);
        var sourceFilesDestinationPath = remoteProjectPath.resolve("source_files");
        verify(sourceFilesToRemoteService).transferFiles(eq(sourceFilesDestinationPath));
        verify(sshClientService).executeRemoteCommand(remoteArgumentCaptor.capture());

        var arguments = remoteArgumentCaptor.getValue().split(" ", 3);
        assertEquals("sbatch", arguments[0]);
        assertEquals(remoteJobScriptsPath.resolve("velocicroptor_job.sh").toString(), arguments[1]);

        // Parse the config (with outer quotes trimmed off)
        ObjectMapper mapper = new ObjectMapper();
        var config = mapper.readTree(arguments[2].substring(1, arguments[2].length() - 1));
        assertEquals(jobId, config.get("job_id").asText());
        assertEquals("velocicroptor", config.get("job_name").asText());
        assertEquals(project.getProjectName(), config.get("chompb_proj_name").asText());
        assertEquals(ADMIN_EMAIL, config.get("admin_address").asText());
        assertEquals(USERNAME, config.get("username").asText());
        assertEquals(USER_EMAIL, config.get("email_address").asText());
        assertFalse(config.get("start_time").asText().isEmpty());
        assertEquals(outputPath.toString(), config.get("output_path").asText());
        assertEquals(OUTPUT_SERVER, config.get("output_server").asText());
    }

    @Test
    public void runFailsTransferFailsTest() throws Exception {
        var options = new ProcessSourceFilesOptions();
        options.setActionName("velocicroptor");
        options.setUsername(USERNAME);
        options.setEmailAddress(USER_EMAIL);

        doThrow(new IOException("Failed to transfer files")).when(sourceFilesToRemoteService).transferFiles(any());

        assertThrows(MigrationException.class, () -> job.run(options));
    }
}
