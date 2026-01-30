package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.exceptions.StateAlreadyExistsException;
import edu.unc.lib.boxc.migration.cdm.model.DestinationsInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.DestinationMappingOptions;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import edu.unc.lib.boxc.migration.cdm.validators.DestinationsValidator;
import edu.unc.lib.boxc.search.api.SearchFieldKey;
import edu.unc.lib.boxc.search.api.exceptions.SolrRuntimeException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.slf4j.Logger;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
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
    private DestinationsService destinationsService;
    private String solrServerUrl;
    private HttpSolrClient solr;

    public static final String PID_KEY = SearchFieldKey.ID.getSolrField();
    public static final String COLLECTION_ID = SearchFieldKey.COLLECTION_ID.getSolrField();
    public static final String RESOURCE_TYPE = SearchFieldKey.RESOURCE_TYPE.getSolrField();

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
            // skip over values from children of compound objects, since they must
            // go to the same destination as their parent work
            ResultSet rs = stmt.executeQuery("select distinct " + options.getFieldName()
                    + " from " + CdmIndexService.TB_NAME
                    + " where " + " (" + CdmIndexService.ENTRY_TYPE_FIELD + " != '"
                    + CdmIndexService.ENTRY_TYPE_COMPOUND_CHILD + "'" +
                    " OR " + CdmIndexService.ENTRY_TYPE_FIELD + " = '" + CdmIndexService.ENTRY_TYPE_DOCUMENT_PDF + "'" +
                    " OR " + CdmIndexService.ENTRY_TYPE_FIELD + " is null)");
            while (rs.next()) {
                if (!rs.getString(1).isEmpty()) {
                    collectionNumbers.add(rs.getString(1));
                }
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
    public Map<String, String> generateCollectionNumbersToPidMapping(DestinationMappingOptions options) {
        Map<String, String> mapCollNumToPid = new HashMap<>();

        List<String> listCollectionNumbers = generateCollectionNumbersList(options);

        for (String collNum : listCollectionNumbers) {
            if (!collNum.isEmpty()) {
                String collNumQuery =  COLLECTION_ID + ":" + collNum;
                SolrQuery query = new SolrQuery();
                query.set("q", collNumQuery);
                query.setFilterQueries(RESOURCE_TYPE + ":Collection");

                try {
                    QueryResponse response = solr.query(query);
                    SolrDocumentList results = response.getResults();
                    if (results.isEmpty()) {
                        mapCollNumToPid.put(collNum, null);
                    } else {
                        mapCollNumToPid.put(collNum, results.get(0).getFieldValue(PID_KEY).toString());
                    }
                } catch (SolrServerException | IOException e) {
                    throw new SolrRuntimeException(e);
                }
            }
        }
        return mapCollNumToPid;
    }

    /**
     * Add archival collection mapping(s) to destination mappings file
     * @param options
     * @throws Exception
     */
    public void addArchivalCollectionMappings(DestinationMappingOptions options) throws IOException {
        Path destinationMappingsPath = project.getDestinationMappingsPath();
        ensureMappingState(options.isForce());
        DestinationsValidator.assertValidDestination(options.getDefaultDestination());

        if (options.getFieldName() != null) {
            try (
                    BufferedWriter writer = Files.newBufferedWriter(destinationMappingsPath,
                            StandardOpenOption.APPEND,
                            StandardOpenOption.CREATE);
                    CSVPrinter csvPrinter = new CSVPrinter(writer,
                            CSVFormat.Builder.create().setHeader(DestinationsInfo.CSV_HEADERS).build());
            ) {
                Map<String, String> mapCollNumToPid = generateCollectionNumbersToPidMapping(options);
                for (Map.Entry<String, String> entry : mapCollNumToPid.entrySet()) {
                    String collNum = entry.getKey();
                    String pid = entry.getValue();

                    // destinations.csv columns: id, destination, collection
                    // if the boxc pid is not null, destination field = the boxc pid and collection field is empty
                    // if the boxc pid is null, destination field is empty and collection field = <coll_num>
                    // the user will need to fill in the destination value, which would be a boxc AdminUnit pid
                    if (pid != null) {
                        csvPrinter.printRecord(options.getFieldName() + ":" + collNum,
                                pid,
                                "");
                    } else {
                        csvPrinter.printRecord(options.getFieldName() + ":" + collNum,
                                "",
                                collNum);
                    }
                }
                if (options.getDefaultDestination() != null) {
                    csvPrinter.printRecord(DestinationsInfo.DEFAULT_ID,
                            options.getDefaultDestination(),
                            options.getDefaultCollection());
                }
            }
            project.getProjectProperties().setDestinationsGeneratedDate(Instant.now());
            ProjectPropertiesSerialization.write(project);
        } else {
            throw new IllegalArgumentException("Field option is empty");
        }
    }

    private void ensureMappingState(boolean force) {
        if (Files.exists(project.getDestinationMappingsPath())) {
            if (force) {
                try {
                    destinationsService.removeMappings();
                } catch (IOException e) {
                    throw new MigrationException("Failed to overwrite destinations file", e);
                }
            } else {
                throw new StateAlreadyExistsException("Cannot create destinations, a file already exists."
                        + " Use the force flag to overwrite.");
            }
        }
    }

    public void setProject(MigrationProject project) {
        this.project = project;
    }

    public void setIndexService(CdmIndexService indexService) {
        this.indexService = indexService;
    }

    public void setDestinationsService(DestinationsService destinationsService) {
        this.destinationsService = destinationsService;
    }

    public void setSolrServerUrl(String solrServerUrl) {
        this.solrServerUrl = solrServerUrl;
    }

    public void setSolr(HttpSolrClient solr) {
        this.solr = solr;
    }
}
