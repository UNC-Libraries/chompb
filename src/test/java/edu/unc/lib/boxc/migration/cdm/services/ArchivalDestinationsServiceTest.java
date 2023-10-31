package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.AbstractOutputTest;
import edu.unc.lib.boxc.migration.cdm.exceptions.StateAlreadyExistsException;
import edu.unc.lib.boxc.migration.cdm.model.DestinationsInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.DestinationMappingOptions;
import edu.unc.lib.boxc.migration.cdm.test.BxcEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.SipServiceHelper;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
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

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
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
        service.setDestinationsService(testHelper.getDestinationsService());
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
        solrResponseWithoutPid();

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
        solrResponseWithPid();

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

    @Test
    public void newDestinationsFileWithPid() throws Exception {
        solrResponseWithPid();
        Path destinationMappingsPath = project.getDestinationMappingsPath();

        var options = new DestinationMappingOptions();
        options.setFieldName("dmrecord");

        service.addArchivalCollectionMappings(options);
        assertTrue(Files.exists(destinationMappingsPath));

        try (
                Reader reader = Files.newBufferedReader(destinationMappingsPath);
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                        .withFirstRecordAsHeader()
                        .withHeader(FieldUrlAssessmentService.URL_CSV_HEADERS)
                        .withTrim());
        ) {
            List<CSVRecord> rows = csvParser.getRecords();
            assertEquals("", rows.get(0).get(1));
            assertEquals("40147", rows.get(1).get(1));
        }
    }

    @Test
    public void newDestinationsFileWithoutPid() throws Exception {
        solrResponseWithoutPid();
        Path destinationMappingsPath = project.getDestinationMappingsPath();

        var options = new DestinationMappingOptions();
        options.setFieldName("dmrecord");

        service.addArchivalCollectionMappings(options);

        try (
                Reader reader = Files.newBufferedReader(destinationMappingsPath);
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                        .withFirstRecordAsHeader()
                        .withHeader(FieldUrlAssessmentService.URL_CSV_HEADERS)
                        .withTrim());
        ) {
            List<CSVRecord> rows = csvParser.getRecords();
            assertEquals("", rows.get(0).get(1));
            assertEquals("", rows.get(1).get(1));
        }
    }

    @Test
    public void existingDestinationsFileWithoutForceFlag() throws Exception {
        solrResponseWithPid();
        writeCsv(mappingBody("aid:40126,bdbd99af-36a5-4bab-9785-e3a802d3737e,"));

        var options = new DestinationMappingOptions();
        options.setFieldName("dmrecord");

        try {
            service.addArchivalCollectionMappings(options);
            fail();
        } catch (StateAlreadyExistsException e) {
            assertTrue(e.getMessage().contains("Cannot create destinations, a file already exists."
                    + " Use the force flag to overwrite."));
        }
    }

    @Test
    public void existingDestinationsFileWithForceFlag() throws Exception {
        solrResponseWithPid();
        writeCsv(mappingBody("aid:40126,bdbd99af-36a5-4bab-9785-e3a802d3737e,"));

        var options = new DestinationMappingOptions();
        options.setFieldName("dmrecord");
        options.setForce(true);

        service.addArchivalCollectionMappings(options);

    }

    @Test
    public void archivalCollectionMappingsWithoutFieldOption() throws Exception {
        var options = new DestinationMappingOptions();

        try {
            service.addArchivalCollectionMappings(options);
            fail();
        } catch (Exception e){
            assertTrue(e.getMessage().contains("Field option is empty"));
        }
    }

    private void solrResponseWithoutPid() throws Exception {
        testHelper.indexExportData("mini_keepsakes");
        QueryResponse testResponse = new QueryResponse();
        testResponse.setResponse(new NamedList<>(Map.of("response", new SolrDocumentList())));
        when(solrClient.query(solrQueryCaptor.capture())).thenReturn(testResponse);
    }

    private void solrResponseWithPid() throws Exception {
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
    }

    private String mappingBody(String... rows) {
        return String.join(",", DestinationsInfo.CSV_HEADERS) + "\n"
                + String.join("\n", rows);
    }

    private void writeCsv(String mappingBody) throws IOException {
        FileUtils.write(project.getDestinationMappingsPath().toFile(),
                mappingBody, StandardCharsets.UTF_8);
        project.getProjectProperties().setDestinationsGeneratedDate(Instant.now());
        ProjectPropertiesSerialization.write(project);
    }
}
