package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.AddSourceFileMappingOptions;
import edu.unc.lib.boxc.migration.cdm.test.BxcEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.TestSshServer;
import edu.unc.lib.boxc.migration.cdm.util.SshClientService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author bbpennel
 */
public class SourceFilesToRemoteServiceTest {
    private static final String PROJECT_NAME = "proj";
    private SourceFilesToRemoteService service;
    private SourceFileService sourceFileService;
    private SshClientService sshClientService;
    private TestSshServer testSshServer;
    private MigrationProject project;
    private Path projPath;
    private Path remotePath;
    private Path clientKeyPath;

    @TempDir
    public Path tmpFolder;

    @BeforeEach
    public void setUp() throws Exception {
        projPath = tmpFolder.resolve(PROJECT_NAME);
        Files.createDirectories(projPath);
        remotePath = tmpFolder.resolve("remote");
        project = MigrationProjectFactory.createCdmMigrationProject(
                projPath, PROJECT_NAME, null, "user",
                CdmEnvironmentHelper.DEFAULT_ENV_ID, BxcEnvironmentHelper.DEFAULT_ENV_ID);
        sourceFileService = new SourceFileService();
        sourceFileService.setProject(project);
        clientKeyPath = Paths.get("src/test/resources/test_client_key");
        testSshServer = new TestSshServer();
        testSshServer.setClientKeyPath(clientKeyPath);
        sshClientService = new SshClientService();
        sshClientService.setSshPort(42222);
        sshClientService.setSshHost("127.0.0.1");
        sshClientService.setSshUsername("testuser");
        sshClientService.setSshKeyPath(clientKeyPath);
        sshClientService.initialize();
        service = new SourceFilesToRemoteService();
        service.setConcurrentTransfers(2);
        service.setSourceFileService(sourceFileService);
        service.setSshClientService(sshClientService);
        testSshServer.startServer();
    }

    @AfterEach
    public void cleanup() throws Exception {
        testSshServer.stopServer();
    }

    @Test
    public void testTransferFiles() throws Exception {
        var filePath1 = createTestFile("sources/file1.jpg", "file1");
        var filePath2 = createTestFile("sources/nest/path/file2.jpg", "file2");
        var filePath3 = createTestFile("sources/nest/path/file3.jpg", "file3");
        var filePath4 = createTestFile("sources/nest/file4.jpg", "file4");
        var filePath5 = createTestFile("sources/another/file5.jpg", "file5");
        AddSourceFileMappingOptions options = new AddSourceFileMappingOptions();
        options.setBasePath(tmpFolder.resolve("sources"));
        options.setExtensions(Arrays.asList("jpg"));
        sourceFileService.addToMapping(options);

        service.transferFiles(remotePath);

        // Verify that the files were transferred
        assertTransferred(filePath1);
        assertTransferred(filePath2);
        assertTransferred(filePath3);
        assertTransferred(filePath4);
        assertTransferred(filePath5);
    }

    @Test
    public void testTransferFilesAuthFailure() throws Exception {
        createTestFile("sources/file1.jpg", "file1");
        createTestFile("sources/nest/path/file2.jpg", "file2");
        AddSourceFileMappingOptions options = new AddSourceFileMappingOptions();
        options.setBasePath(tmpFolder.resolve("sources"));
        options.setExtensions(Arrays.asList("jpg"));
        sourceFileService.addToMapping(options);

        Path badClientKey = Paths.get("src/test/resources/test_client2_key");
        sshClientService.setSshKeyPath(badClientKey);
        sshClientService.initialize();

        var e = assertThrows(MigrationException.class, () -> service.transferFiles(remotePath));
        assertTrue(e.getMessage().contains("Authentication to server failed"));

        // Verify no files transferred
        assertFalse(Files.exists(remotePath));
    }

    @Test
    public void testTransferFilesReservedCharacters() throws Exception {
        var filePath1 = createTestFile("sources/fil e1.jpg", "file1");
        var filePath2 = createTestFile("sources/space path/file2.jpg", "file2");
        var filePath3 = createTestFile("sources/nest/pa'th/fi&l;e3.jpg", "file3");
        var filePath4 = createTestFile("sources/ne(st/file)4.jpg", "file4");
        var filePath5 = createTestFile("sources/anot!her/f*i\"le'5.jpg", "file5");
        AddSourceFileMappingOptions options = new AddSourceFileMappingOptions();
        options.setBasePath(tmpFolder.resolve("sources"));
        options.setExtensions(Arrays.asList("jpg"));
        sourceFileService.addToMapping(options);

        service.transferFiles(remotePath);

        // Verify that the files were transferred
        assertTransferred(filePath1);
        assertTransferred(filePath2);
        assertTransferred(filePath3);
        assertTransferred(filePath4);
        assertTransferred(filePath5);
    }

    private Path createTestFile(String relativePath, String content) throws Exception {
        Path file = tmpFolder.resolve(relativePath);
        Files.createDirectories(file.getParent());
        Files.writeString(file, content);
        return file;
    }

    private void assertTransferred(Path sourcePath) throws IOException {
        Path remoteFile = remotePath.resolve(sourcePath.toString().substring(1));
        assertTrue(Files.exists(remoteFile));
        assertEquals(Files.readString(remoteFile), Files.readString(sourcePath));
    }
}
