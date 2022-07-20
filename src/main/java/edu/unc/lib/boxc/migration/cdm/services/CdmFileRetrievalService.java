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
import edu.unc.lib.boxc.migration.cdm.model.CdmEnvironment;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.ChompbConfigService.ChompbConfig;
import org.apache.sshd.client.SshClient;
import org.apache.sshd.common.config.keys.FilePasswordProvider;
import org.apache.sshd.scp.client.ScpClient;
import org.apache.sshd.scp.client.ScpClientCreator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

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
    public static final String IMAGE_SUBPATH = "image";
    private static final String CPD_SUBPATH = IMAGE_SUBPATH + "/*.cpd";
    public static final String CPD_EXPORT_PATH = "cpds";
    public static final String EXPORTED_SOURCE_FILES_DIR = "source_files";

    private String sshUsername;
    private String sshPassword;
    private ChompbConfig chompbConfig;

    /**
     * Download the desc.all file for the collection being migrated
     */
    public void downloadDescAllFile() {
        executeDownloadBlock((scpClient) -> {
            var remotePath = getSshCollectionPath().resolve(DESC_SUBPATH).toString();
            try {
                scpClient.download(remotePath, getDescAllPath(project));
            } catch (IOException e) {
                throw new MigrationException("Failed to download desc.all file", e);
            }
        });
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
        executeDownloadBlock((scpClient) -> {
            var remotePath = getSshCollectionPath().resolve(CPD_SUBPATH).toString();
            try {
                scpClient.download(remotePath, cpdsPath);
            } catch (IOException e) {
                throw new MigrationException("Failed to download cpd files", e);
            }
        });
    }

    public static Path getExportedCpdsPath(MigrationProject project) {
        return project.getExportPath().resolve(CPD_EXPORT_PATH);
    }

    /**
     * Perform the provided download operations with a ScpClient
     * @param downloadBlock method containing download operations
     */
    public void executeDownloadBlock(Consumer<ScpClient> downloadBlock) {
        SshClient client = SshClient.setUpDefaultClient();
        client.setFilePasswordProvider(FilePasswordProvider.of(sshPassword));
        client.start();
        var cdmEnvId = project.getProjectProperties().getCdmEnvironmentId();
        var cdmEnvConfig = chompbConfig.getCdmEnvironments().get(cdmEnvId);
        try (var sshSession = client.connect(sshUsername, cdmEnvConfig.getSshHost(), cdmEnvConfig.getSshPort())
                .verify(SSH_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .getSession()) {
            sshSession.addPasswordIdentity(sshPassword);

            sshSession.auth().verify(SSH_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            var scpClientCreator = ScpClientCreator.instance();
            var scpClient = scpClientCreator.createScpClient(sshSession);
            downloadBlock.accept(scpClient);
        } catch (IOException e) {
            throw new MigrationException("Failed to establish ssh session", e);
        }
    }

    private CdmEnvironment getCdmEnvironment() {
        var cdmEnvId = project.getProjectProperties().getCdmEnvironmentId();
        return chompbConfig.getCdmEnvironments().get(cdmEnvId);
    }

    public Path getSshCollectionPath() {
        var cdmEnvConfig = getCdmEnvironment();
        String collectionId = project.getProjectProperties().getCdmCollectionId();
        return Paths.get(cdmEnvConfig.getSshDownloadBasePath(), collectionId);
    }

    /**
     * @param project
     * @return Path where the exported source files are stored
     */
    public static Path getExportedSourceFilesPath(MigrationProject project) {
        return project.getExportPath().resolve(EXPORTED_SOURCE_FILES_DIR);
    }

    public void setProject(MigrationProject project) {
        this.project = project;
    }

    public void setSshUsername(String sshUsername) {
        this.sshUsername = sshUsername;
    }

    public void setSshPassword(String sshPassword) {
        this.sshPassword = sshPassword;
    }

    public void setChompbConfig(ChompbConfig chompbConfig) {
        this.chompbConfig = chompbConfig;
    }
}
