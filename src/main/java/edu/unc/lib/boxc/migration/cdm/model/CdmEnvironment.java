package edu.unc.lib.boxc.migration.cdm.model;

/**
 * Configuration information for a cdm environment
 *
 * @author bbpennel
 */
public class CdmEnvironment {
    private String sshHost;
    private int sshPort;
    private String httpBaseUrl;
    private String sshDownloadBasePath;

    public String getSshHost() {
        return sshHost;
    }

    public void setSshHost(String sshHost) {
        this.sshHost = sshHost;
    }

    public int getSshPort() {
        return sshPort;
    }

    public void setSshPort(int sshPort) {
        this.sshPort = sshPort;
    }

    public String getSshDownloadBasePath() {
        return sshDownloadBasePath;
    }

    public void setSshDownloadBasePath(String sshDownloadBasePath) {
        this.sshDownloadBasePath = sshDownloadBasePath;
    }

    public String getHttpBaseUrl() {
        return httpBaseUrl;
    }

    public void setHttpBaseUrl(String httpBaseUrl) {
        this.httpBaseUrl = httpBaseUrl;
    }
}
