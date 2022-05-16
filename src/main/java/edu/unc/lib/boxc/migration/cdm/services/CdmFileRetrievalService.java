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
package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import org.apache.sshd.client.SshClient;
import org.apache.sshd.common.config.keys.FilePasswordProvider;
import org.apache.sshd.scp.client.ScpClientCreator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

/**
 * Service for retrieving CDM files directly from a remote server file system
 *
 * @author bbpennel
 */
public class CdmFileRetrievalService {
    private MigrationProject project;

    private static final int SSH_TIMEOUT_SECONDS = 10;
    private static final String DESC_SUBPATH = "index/description/desc.all";
    public static final String DESC_ALL_FILENAME = "desc.all";
    private static final String CPD_SUBPATH = "image/*.cpd";
    public static final String CPD_EXPORT_PATH = "cpds";

    private String downloadBasePath;
    private String sshUsername;
    private String cdmHost;
    private String sshPassword;
    private int sshPort;

    /**
     * Download the desc.all file for the collection being migrated
     */
    public void downloadDescAllFile() {
        downloadFiles(DESC_SUBPATH, getDescAllPath(project));
    }

    /**
     * @param project
     * @return Path where the exported desc all file is stored
     */
    public static Path getDescAllPath(MigrationProject project) {
        return project.getExportPath().resolve(DESC_ALL_FILENAME);
    }

    /**
     * Download all cpd files
     */
    public void downloadCpdFiles() {
        var cpdsPath = getExportedCpdsPath(project);
        try {
            // Ensure that the CPD folder exists
            Files.createDirectories(cpdsPath);
        } catch (IOException e) {
            throw new MigrationException("Failed to create CPD export directory", e);
        }
        downloadFiles(CPD_SUBPATH, cpdsPath);
    }

    public static Path getExportedCpdsPath(MigrationProject project) {
        return project.getExportPath().resolve(CPD_EXPORT_PATH);
    }

    public void downloadFiles(String remoteSubPath, Path localDestination) {
        SshClient client = SshClient.setUpDefaultClient();
        client.setFilePasswordProvider(FilePasswordProvider.of(sshPassword));
        client.start();
        try (var sshSession = client.connect(sshUsername, cdmHost, sshPort)
                .verify(SSH_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .getSession()) {
            sshSession.addPasswordIdentity(sshPassword);

            sshSession.auth().verify(SSH_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            var scpClientCreator = ScpClientCreator.instance();
            var scpClient = scpClientCreator.createScpClient(sshSession);
            String collectionId = project.getProjectProperties().getCdmCollectionId();
            var remotePath = Paths.get(downloadBasePath, collectionId, remoteSubPath).toString();
            scpClient.download(remotePath, localDestination);
        } catch (IOException e) {
            throw new MigrationException("Failed to download desc.all file", e);
        }
    }

    public void setProject(MigrationProject project) {
        this.project = project;
    }

    public void setDownloadBasePath(String downloadBasePath) {
        this.downloadBasePath = downloadBasePath;
    }

    public void setSshUsername(String sshUsername) {
        this.sshUsername = sshUsername;
    }

    public void setCdmHost(String cdmHost) {
        this.cdmHost = cdmHost;
    }

    public void setSshPassword(String sshPassword) {
        this.sshPassword = sshPassword;
    }

    public void setSshPort(int sshPort) {
        this.sshPort = sshPort;
    }
}
