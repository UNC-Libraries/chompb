package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.DestinationMappingOptions;
import edu.unc.lib.boxc.search.api.exceptions.SolrRuntimeException;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.slf4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Service for destination matching based on archival collection number
 * @author krwong
 */
public class ArchivalDestinationsService {
    private static final Logger log = getLogger(ArchivalDestinationsService.class);

    private MigrationProject project;
    private CdmIndexService indexService;
    private String solrServerUrl;
    private HttpSolrClient solr;

    public void initialize() {
        solr = new HttpSolrClient.Builder(solrServerUrl).build();
    }

    /**
     * Generates a unique list of values in the accepted field name
     * @param options destination mapping options
     * @return A list
     */
    public List<String> generateCollectionNumbersList(DestinationMappingOptions options) {
        List<String> collectionNumbers = new ArrayList<>();

        Connection conn = null;
        try {
            conn = indexService.openDbConnection();
            Statement stmt = conn.createStatement();
            // skip over values from children of compound objects, since they must go to the same destination as their parent work
            ResultSet rs = stmt.executeQuery("select distinct " + options.getFieldName()
                    + " from " + CdmIndexService.TB_NAME
                    + " where " + " ("+ CdmIndexService.ENTRY_TYPE_FIELD + " != '"
                    + CdmIndexService.ENTRY_TYPE_COMPOUND_CHILD + "'" +
                    " OR " + CdmIndexService.ENTRY_TYPE_FIELD + " is null)");
            while (rs.next()) {
                collectionNumbers.add(rs.getString(1));
            }
            return collectionNumbers;
        } catch (SQLException e) {
            throw new MigrationException("Error interacting with export index", e);
        } finally {
            CdmIndexService.closeDbConnection(conn);
        }
    }

    /**
     * Generates a map of archival collection numbers to boxc pids
     * @param options destination mapping options
     * @return A map
     */
    public Map<String, String> generateCollectionNumbersToPidMapping(DestinationMappingOptions options) throws Exception {
        Map<String, String> mapCollNumToPid = new HashMap<>();

        List<String> listCollectionNumbers = generateCollectionNumbersList(options);

        for (String collNum : listCollectionNumbers) {
            String collNumQuery = "collectionId:" + collNum;
            SolrQuery query = new SolrQuery();
            query.set("q", collNumQuery);
            query.setFilterQueries("resourceType:Collection");

            try {
                QueryResponse response = solr.query(query);
                SolrDocumentList results = response.getResults();
                if (results.isEmpty()) {
                    mapCollNumToPid.put(collNum, null);
                } else {
                    mapCollNumToPid.put(collNum, results.get(0).getFieldValue("pid").toString());
                }
            } catch (SolrServerException e) {
                throw new SolrRuntimeException(e);
            }
        }
        return mapCollNumToPid;
    }

    public void setProject(MigrationProject project) {
        this.project = project;
    }

    public void setIndexService(CdmIndexService indexService) {
        this.indexService = indexService;
    }

    public void setSolrServerUrl(String solrServerUrl) {
        this.solrServerUrl = solrServerUrl;
    }

    public void setSolr(HttpSolrClient solr) {
        this.solr = solr;
    }
}
