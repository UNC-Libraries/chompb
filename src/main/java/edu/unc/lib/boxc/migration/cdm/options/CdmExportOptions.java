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
package edu.unc.lib.boxc.migration.cdm.options;

import picocli.CommandLine;

/**
 * Options for CDM export operation
 *
 * @author bbpennel
 */
public class CdmExportOptions {
    @CommandLine.Option(names = { "--cdm-url" },
            description = {"Base URL to the CDM web service API. Falls back to CDM_BASE_URL env variable.",
                    "Default: ${DEFAULT-VALUE}"},
            defaultValue = "${env:CDM_BASE_URL:-http://localhost:82/}")
    private String cdmBaseUri;
    @CommandLine.Option(names = { "-u", "--cdm-user"},
            description = {"User name for CDM requests.",
                    "Defaults to current user: ${DEFAULT-VALUE}"},
            defaultValue = "${sys:user.name}")
    private String cdmUsername;
    @CommandLine.Option(names = {"-p", "--cdm-password"},
            description = "Password for CDM requests. Required.",
            arity = "0..1",
            interactive = true)
    private String cdmPassword;
    public static final int MAX_EXPORT_RECORDS_PER_PAGE = 5000;
    @CommandLine.Option(names = {"-n", "--records-per-page"},
            description = {"Page size for exports.",
                    "Default: ${DEFAULT-VALUE}. Max page size is " + MAX_EXPORT_RECORDS_PER_PAGE},
            defaultValue = "500")
    private int pageSize = 500;
    @CommandLine.Option(names = { "-f", "--force"},
            description = "Force the export to restart from the beginning. Use if a previous export was started "
                    + "or completed, but you would like to begin the export again.")
    private boolean force;
    @CommandLine.Option(names = { "-H", "--ssh-host"},
            description = {"Host name of the CDM SSH server.",
                    "Default: ${DEFAULT-VALUE}"},
            defaultValue = "${env:CDM_SSH_HOST:-127.0.0.1}")
    private String cdmSshHost;
    @CommandLine.Option(names = { "-P", "--ssh-port"},
            description = {"Port of the CDM SSH server.",
                    "Default: ${DEFAULT-VALUE}"},
            defaultValue = "22")
    private int cdmSshPort;
    @CommandLine.Option(names = { "-D", "--download-path"},
            description = {"Remote base path where CDM files should be located for transfer via SSH.",
                    "Default: ${DEFAULT-VALUE}"},
            defaultValue = "${env:CDM_SSH_DOWNLOAD_PATH}")
    private String cdmSshDownloadBasePath;

    public String getCdmBaseUri() {
        return cdmBaseUri;
    }

    public void setCdmBaseUri(String cdmBaseUri) {
        this.cdmBaseUri = cdmBaseUri;
    }

    public String getCdmUsername() {
        return cdmUsername;
    }

    public void setCdmUsername(String cdmUsername) {
        this.cdmUsername = cdmUsername;
    }

    public String getCdmPassword() {
        return cdmPassword;
    }

    public void setCdmPassword(String cdmPassword) {
        this.cdmPassword = cdmPassword;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public String getCdmSshHost() {
        return cdmSshHost;
    }

    public void setCdmSshHost(String cdmSshHost) {
        this.cdmSshHost = cdmSshHost;
    }

    public int getCdmSshPort() {
        return cdmSshPort;
    }

    public void setCdmSshPort(int cdmSshPort) {
        this.cdmSshPort = cdmSshPort;
    }

    public String getCdmSshDownloadBasePath() {
        return cdmSshDownloadBasePath;
    }

    public void setCdmSshDownloadBasePath(String cdmSshDownloadBasePath) {
        this.cdmSshDownloadBasePath = cdmSshDownloadBasePath;
    }

    public boolean isForce() {
        return force;
    }

    public void setForce(boolean force) {
        this.force = force;
    }
}
