package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo;
import edu.unc.lib.boxc.migration.cdm.util.SshClientService;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Collectors;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Service for transferring source files from the local server to a remote destination.
 * @author bbpennel
 */
public class SourceFilesToRemoteService {
    private static final Logger log = getLogger(SourceFilesToRemoteService.class);
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
        var pathsDeque = sourceMappings.getMappings().stream()
                .map(SourceFilesInfo.SourceFileMapping::getFirstSourcePath)
                .collect(Collectors.toCollection(ConcurrentLinkedDeque::new));
        // For tracking if a parent directory has already been created
        Set<String> createdParentsSet = ConcurrentHashMap.newKeySet();
        // Create the remote destination directory
        log.info("Creating remote destination directory {}", destinationBasePath);
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
        // Create the parent path if we haven't already done so
        // Upload the file to the appropriate path on the remote server
        return new Thread(() -> {
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
                            log.debug("Creating missing parent directory {}", destParentPath);
                            createdParentsSet.add(destParentPath.toString());
                            sshClientService.executeRemoteCommand("mkdir -p " + destPath.getParent());
                        }
                    }
                    // Upload the file to the appropriate path on the remote server
                    sshClientService.executeScpBlock(sshClient, (scpClient) -> {
                        try {
                            log.info("Transferring file {} to {}", sourcePath, destPath);
                            scpClient.upload(sourcePath.toString(), destPath.toString());
                        } catch (IOException e) {
                            throw new RuntimeException("Failed to transfer file " + sourcePath, e);
                        }
                    });
                });
            }
        });
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
