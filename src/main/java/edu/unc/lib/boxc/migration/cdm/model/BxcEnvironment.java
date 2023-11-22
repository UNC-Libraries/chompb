package edu.unc.lib.boxc.migration.cdm.model;

/**
 * Configuration information for a Box-c environment
 *
 * @author bbpennel
 */
public class BxcEnvironment {
    private String httpBaseUrl;
    private String solrServerUrl;
    private String baseSolrUrl;

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

    public String getBaseSolrUrl() {
        return baseSolrUrl;
    }

    public void setBaseSolrUrl(String baseSolrUrl) {
        this.baseSolrUrl = baseSolrUrl;
    }
}
