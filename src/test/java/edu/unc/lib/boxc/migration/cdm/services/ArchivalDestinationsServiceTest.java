package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.DestinationMappingOptions;
import edu.unc.lib.boxc.migration.cdm.test.BxcEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.SipServiceHelper;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.mockito.MockitoAnnotations.openMocks;
import static org.mockito.Mockito.when;

public class ArchivalDestinationsServiceTest {
    private static final String PROJECT_NAME = "proj";
    private static final String SOLR_URL = "http://example.com:88/solr";

    @Captor
    private ArgumentCaptor<SolrQuery> solrQueryCaptor;

    @TempDir
    public Path tmpFolder;

    private SipServiceHelper testHelper;
    private MigrationProject project;
    private ArchivalDestinationsService service;
    private AutoCloseable closeable;

    @Mock
    private HttpSolrClient solrClient;

    @BeforeEach
    public void setup() throws Exception {
        closeable = openMocks(this);
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder, PROJECT_NAME, null, "user",
                CdmEnvironmentHelper.DEFAULT_ENV_ID, BxcEnvironmentHelper.DEFAULT_ENV_ID);
        testHelper = new SipServiceHelper(project, tmpFolder);
        service = new ArchivalDestinationsService();
        service.setProject(project);
        service.setIndexService(testHelper.getIndexService());
        service.setSolrServerUrl(SOLR_URL);
        service.setSolr(solrClient);
    }

    @AfterEach
    void closeService() throws Exception {
        closeable.close();
    }

    @Test
    public void archivalCollectionNumberTest() throws Exception {
        testHelper.indexExportData("mini_keepsakes");

        var options = new DestinationMappingOptions();
        options.setFieldName("dmrecord");

        var result = service.generateCollectionNumbersList(options);
        assertIterableEquals(Arrays.asList("216", "604", "607"), result);
    }

    @Test
    public void collectionNumberToNullPidTest() throws Exception {
        testHelper.indexExportData("mini_keepsakes");
        QueryResponse testResponse = new QueryResponse();
        testResponse.setResponse(new NamedList<>(Map.of("response", new SolrDocumentList())));
        when(solrClient.query(solrQueryCaptor.capture())).thenReturn(testResponse);

        Map<String, String> expected = new HashMap<>();
        expected.put("216", null);
        expected.put("604", null);
        expected.put("607", null);

        var options = new DestinationMappingOptions();
        options.setFieldName("dmrecord");

        var result = service.generateCollectionNumbersToPidMapping(options);
        assertEquals(expected, result);
        var solrValues = solrQueryCaptor.getAllValues();
        assertEquals(3, solrValues.size());
        assertEquals("collectionId:216", solrValues.get(0).getQuery());
        assertEquals("collectionId:604", solrValues.get(1).getQuery());
        assertEquals("collectionId:607", solrValues.get(2).getQuery());
        assertEquals("resourceType:Collection",
                Arrays.stream(solrValues.get(0).getFilterQueries()).findFirst().get());
    }

    @Test
    public void collectionNumberToPidTest() throws Exception {
        testHelper.indexExportData("mini_keepsakes");

        QueryResponse testResponseA = new QueryResponse();
        SolrDocument testDocumentA = new SolrDocument();
        testDocumentA.addField("pid", "40147");
        SolrDocumentList testListA = new SolrDocumentList();
        testListA.add(testDocumentA);
        testResponseA.setResponse(new NamedList<>(Map.of("response", testListA)));

        QueryResponse testResponseB = new QueryResponse();
        testResponseB.setResponse(new NamedList<>(Map.of("response", new SolrDocumentList())));

        when(solrClient.query(solrQueryCaptor.capture())).thenReturn(testResponseA).thenReturn(testResponseB)
                .thenReturn(testResponseA);

        Map<String, String> expected = new HashMap<>();
        expected.put("216", "40147");
        expected.put("604", null);
        expected.put("607", "40147");

        var options = new DestinationMappingOptions();
        options.setFieldName("dmrecord");

        var result = service.generateCollectionNumbersToPidMapping(options);
        assertEquals(expected, result);
        var solrValues = solrQueryCaptor.getAllValues();
        assertEquals(3, solrValues.size());
        assertEquals("collectionId:216", solrValues.get(0).getQuery());
        assertEquals("collectionId:604", solrValues.get(1).getQuery());
        assertEquals("collectionId:607", solrValues.get(2).getQuery());
        assertEquals("resourceType:Collection",
                Arrays.stream(solrValues.get(0).getFilterQueries()).findFirst().get());
    }
}
