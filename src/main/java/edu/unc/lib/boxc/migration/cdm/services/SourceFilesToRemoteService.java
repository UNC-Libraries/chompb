package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo;
import edu.unc.lib.boxc.migration.cdm.util.SshClientService;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Collectors;

/**
 * Service for transferring source files from the local server to a remote destination.
 * @author bbpennel
 */
public class SourceFilesToRemoteService {
    private SourceFileService sourceFileService;
    private SshClientService sshClientService;
    private int concurrentTransfers = 5;

    /**
     * Transfer files from the source CDM server to the remote destination.
     * Files are transferred in parallel, up to concurrentTransfers at a time.
     * @param destinationPath
     * @throws IOException
     */
    public void transferFiles(Path destinationPath) throws IOException {
        var sourceMappings = sourceFileService.loadMappings();
        final Path destinationBasePath = destinationPath.toAbsolutePath();
        // Get all the source paths as a thread safe queue
        var sourcePaths = sourceMappings.getMappings().stream()
                                    .map(SourceFilesInfo.SourceFileMapping::getFirstSourcePath)
                                    .collect(Collectors.toList());
        var pathsDeque = new ConcurrentLinkedDeque<Path>(sourcePaths);
        // For tracking if a parent directory has already been created
        Set<String> createdParentsSet = ConcurrentHashMap.newKeySet();
        // Create the remote destination directory
        sshClientService.executeRemoteCommand("mkdir -p " + destinationBasePath);
        createdParentsSet.add(destinationBasePath.toString());

        var threads = new ArrayList<Thread>(concurrentTransfers);
        // Start threads for parallel transfer of files
        for (int i = 0; i < concurrentTransfers; i++) {
            var thread = createTransferThread(pathsDeque, destinationBasePath, createdParentsSet);
            thread.start();
            threads.add(thread);
        }

        // Wait for all threads to finish
        threads.forEach(t -> {
            try {
                t.join();
            } catch (InterruptedException e) {
                throw new RuntimeException("Thread interrupted", e);
            }
        });
    }

    private Thread createTransferThread(ConcurrentLinkedDeque<Path> pathsDeque,
                                        Path destinationBasePath,
                                        Set<String> createdParentsSet) {
        var thread = new Thread(() -> {
            Path nextPath;
            while ((nextPath = pathsDeque.poll()) != null) {
                final Path sourcePath = nextPath;
                sshClientService.executeSshBlock((sshClient) -> {
                    var sourceRelative = sourcePath.toAbsolutePath().toString().substring(1);
                    var destPath = destinationBasePath.resolve(sourceRelative);
                    var destParentPath = destPath.getParent();
                    // Create the parent path if we haven't already done so
                    synchronized (createdParentsSet) {
                        if (!createdParentsSet.contains(destParentPath.toString())) {
                            createdParentsSet.add(destParentPath.toString());
                            sshClientService.executeRemoteCommand("mkdir -p " + destPath.getParent());
                        }
                    }
                    // Upload the file to the appropriate path on the remote server
                    sshClientService.executeScpBlock(sshClient, (scpClient) -> {
                        try {
                            scpClient.upload(sourcePath.toString(), destPath.toString());
                        } catch (IOException e) {
                            throw new RuntimeException("Failed to transfer file " + sourcePath, e);
                        }
                    });
                });
            }
        });
        return thread;
    }

    public void setSourceFileService(SourceFileService sourceFileService) {
        this.sourceFileService = sourceFileService;
    }

    public void setSshClientService(SshClientService sshClientService) {
        this.sshClientService = sshClientService;
    }

    public void setConcurrentTransfers(int concurrentTransfers) {
        this.concurrentTransfers = concurrentTransfers;
    }
}
