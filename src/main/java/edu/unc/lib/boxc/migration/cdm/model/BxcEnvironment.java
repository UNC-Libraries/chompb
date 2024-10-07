package edu.unc.lib.boxc.migration.cdm.model;

import java.nio.file.Path;

/**
 * Configuration information for a Box-c environment
 *
 * @author bbpennel
 */
public class BxcEnvironment {
    private String httpBaseUrl;
    private String solrServerUrl;

    private String boxctronHost;
    private int boxctronPort;
    private String boxctronSshUser;
    private Path boxctronKeyPath;
    private Path boxctronRemoteProjectsPath;
    private String boxctronAdminEmail;
    private String boxctronOutputServer;
    private Path boxctronOutputBasePath;
    private Path boxctronRemoteJobScriptPath;

    public String getHttpBaseUrl() {
        return httpBaseUrl;
    }

    public void setHttpBaseUrl(String httpBaseUrl) {
        this.httpBaseUrl = httpBaseUrl;
    }

    public String getSolrServerUrl() {
        return solrServerUrl;
    }

    public void setSolrServerUrl(String solrServerUrl) {
        this.solrServerUrl = solrServerUrl;
    }

    public String getBoxctronHost() {
        return boxctronHost;
    }

    public void setBoxctronHost(String boxctronHost) {
        this.boxctronHost = boxctronHost;
    }

    public int getBoxctronPort() {
        return boxctronPort;
    }

    public void setBoxctronPort(int boxctronPort) {
        this.boxctronPort = boxctronPort;
    }

    public String getBoxctronSshUser() {
        return boxctronSshUser;
    }

    public void setBoxctronSshUser(String boxctronSshUser) {
        this.boxctronSshUser = boxctronSshUser;
    }

    public Path getBoxctronKeyPath() {
        return boxctronKeyPath;
    }

    public void setBoxctronKeyPath(Path boxctronKeyPath) {
        this.boxctronKeyPath = boxctronKeyPath;
    }

    public Path getBoxctronRemoteProjectsPath() {
        return boxctronRemoteProjectsPath;
    }

    public void setBoxctronRemoteProjectsPath(Path boxctronRemoteProjectsPath) {
        this.boxctronRemoteProjectsPath = boxctronRemoteProjectsPath;
    }

    public String getBoxctronAdminEmail() {
        return boxctronAdminEmail;
    }

    public void setBoxctronAdminEmail(String boxctronAdminEmail) {
        this.boxctronAdminEmail = boxctronAdminEmail;
    }

    public String getBoxctronOutputServer() {
        return boxctronOutputServer;
    }

    public void setBoxctronOutputServer(String boxctronOutputServer) {
        this.boxctronOutputServer = boxctronOutputServer;
    }

    public Path getBoxctronOutputBasePath() {
        return boxctronOutputBasePath;
    }

    public void setBoxctronOutputBasePath(Path boxctronOutputBasePath) {
        this.boxctronOutputBasePath = boxctronOutputBasePath;
    }

    public Path getBoxctronRemoteJobScriptPath() {
        return boxctronRemoteJobScriptPath;
    }

    public void setBoxctronRemoteJobScriptPath(Path boxctronRemoteJobScriptPath) {
        this.boxctronRemoteJobScriptPath = boxctronRemoteJobScriptPath;
    }
}
