package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo;
import edu.unc.lib.boxc.migration.cdm.util.SshClientService;
import org.apache.poi.util.IOUtils;
import org.apache.sshd.sftp.client.SftpClient;
import org.apache.sshd.sftp.client.SftpClientFactory;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
    private ExecutorService executor;

    /**
     * Transfer files from the source CDM server to the remote destination.
     * Files are transferred in parallel, up to concurrentTransfers at a time.
     * @param destinationPath
     * @throws IOException
     */
    public void transferFiles(Path destinationPath) throws IOException {
        executor = Executors.newFixedThreadPool(concurrentTransfers);

        try {
            var sourceMappings = sourceFileService.loadMappings();
            final Path destinationBasePath = destinationPath.toAbsolutePath();
            var pathsList = sourceMappings.getMappings().stream()
                    .map(SourceFilesInfo.SourceFileMapping::getFirstSourcePath)
                    .collect(Collectors.toList());
            var pathsDeque = new ConcurrentLinkedDeque<>(pathsList);
            // Create the parent path structure before we start transfers
            var parentPaths = listMostSpecificParents(pathsList);
            createParentPaths(parentPaths, destinationBasePath);

            // Capture futures for transfer threads so we can receive errors
            var futures = new ArrayList<Future<?>>();
            for (int i = 0; i < concurrentTransfers; i++) {
                futures.add(executor.submit(createTransferTask(pathsDeque, destinationBasePath)));
            }
            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (ExecutionException e) {
                    throw new RuntimeException(e.getCause());
                }
            }
        } finally {
            executor.shutdown();
        }
    }

    /**
     * @param paths list of paths to get parent directories from
     * @return list of parent directories, deduplicated so that if a directory is a parent of another
     *  then the parent will be removed. So if we have paths:
     *      * /a/b
     *      * /a/b/c
     *      * /d/e/f
     * then just the paths /a/b/c and /d/e/f would be returned
     */
    private List<Path> listMostSpecificParents(List<Path> paths) {
        var parents = paths.stream().map(Path::getParent)
                                    .sorted()
                                    .collect(Collectors.toList());
        List<Path> result = new ArrayList<>();
        for (int i = 0; i < parents.size(); i++) {
            Path current = parents.get(i);
            boolean isParent = false;
            // Check if the current path is a parent of any subsequent path
            for (int j = i + 1; j < parents.size(); j++) {
                if (parents.get(j).startsWith(current)) {
                    isParent = true;
                    break;
                }
            }
            // If not a parent, add to result
            if (!isParent) {
                result.add(current);
            }
        }
        return result;
    }

    private boolean remoteDirectoryExists(SftpClient sftpClient, Path path) {
        try {
            sftpClient.stat(path.toString());
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private void createParentPaths(List<Path> paths, Path destinationBasePath) {
        sshClientService.executeSshBlock((sshClient) -> {
            var previousPaths = new HashSet<String>();
            SftpClientFactory factory = SftpClientFactory.instance();
            try (SftpClient sftpClient = factory.createSftpClient(sshClient)) {

                for (Path path : paths) {
                    var pathRelative = path.toAbsolutePath().toString().substring(1);
                    var destPath = destinationBasePath.resolve(pathRelative).toString();
                    var pathParts = destPath.split("/");
                    var progressivePath = Path.of("/");
                    for (var pathPart: pathParts) {
                        progressivePath = progressivePath.resolve(pathPart);
                        var stringPath = progressivePath.toString();
                        if (previousPaths.contains(stringPath) || remoteDirectoryExists(sftpClient, progressivePath)) {
                            continue;
                        }
                        log.info("Creating parent path {}", stringPath);
                        sftpClient.mkdir(stringPath);
                        previousPaths.add(stringPath);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private Runnable createTransferTask(ConcurrentLinkedDeque<Path> pathsDeque, Path destinationBasePath) {
        return () -> {
            sshClientService.executeSshBlock((sshClient) -> {
                SftpClientFactory factory = SftpClientFactory.instance();
                try (SftpClient sftpClient = factory.createSftpClient(sshClient)) {
                    Path nextPath;
                    while ((nextPath = pathsDeque.poll()) != null) {
                        final Path sourcePath = nextPath;

                        var sourceRelative = sourcePath.toAbsolutePath().toString().substring(1);
                        var destPath = destinationBasePath.resolve(sourceRelative).toString();
                        try (InputStream in = Files.newInputStream(sourcePath)) {
                            // Open output stream to the remote file
                            try (OutputStream out = sftpClient.write(destPath)) {
                                IOUtils.copy(in, out);
                            }
                        }
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        };
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
