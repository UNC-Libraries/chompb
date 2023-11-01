package edu.unc.lib.boxc.migration.cdm.services;

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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
    public void generateCollectionNumbersListTest() throws Exception {
        testHelper.indexExportData("mini_keepsakes");

        var options = new DestinationMappingOptions();
        options.setFieldName("dmrecord");

        var result = service.generateCollectionNumbersList(options);
        assertIterableEquals(Arrays.asList("216", "604", "607"), result);
    }

    @Test
    public void generateCollectionNumbersToPidMappingNullPidTest() throws Exception {
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
        assertEquals(ArchivalDestinationsService.COLLECTION_ID + ":216", solrValues.get(0).getQuery());
        assertEquals(ArchivalDestinationsService.COLLECTION_ID + ":604", solrValues.get(1).getQuery());
        assertEquals(ArchivalDestinationsService.COLLECTION_ID + ":607", solrValues.get(2).getQuery());
        assertEquals(ArchivalDestinationsService.RESOURCE_TYPE + ":Collection",
                Arrays.stream(solrValues.get(0).getFilterQueries()).findFirst().get());
    }

    @Test
    public void generateCollectionNumbersToPidMappingWithPidTest() throws Exception {
        solrResponseWithPid();

        Map<String, String> expected = new HashMap<>();
        expected.put("216", "bdbd99af-36a5-4bab-9785-e3a802d3737e");
        expected.put("604", null);
        expected.put("607", "bdbd99af-36a5-4bab-9785-e3a802d3737e");

        var options = new DestinationMappingOptions();
        options.setFieldName("dmrecord");

        var result = service.generateCollectionNumbersToPidMapping(options);
        assertEquals(expected, result);
        var solrValues = solrQueryCaptor.getAllValues();
        assertEquals(3, solrValues.size());
        assertEquals(ArchivalDestinationsService.COLLECTION_ID + ":216", solrValues.get(0).getQuery());
        assertEquals(ArchivalDestinationsService.COLLECTION_ID + ":604", solrValues.get(1).getQuery());
        assertEquals(ArchivalDestinationsService.COLLECTION_ID + ":607", solrValues.get(2).getQuery());
        assertEquals(ArchivalDestinationsService.RESOURCE_TYPE + ":Collection",
                Arrays.stream(solrValues.get(0).getFilterQueries()).findFirst().get());
    }

    @Test
    public void addArchivalCollectionMappingsWithPidTest() throws Exception {
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
            assertIterableEquals(Arrays.asList("dmrecord:604", "", "604"), rows.get(0));
            assertIterableEquals(Arrays.asList("dmrecord:607", "bdbd99af-36a5-4bab-9785-e3a802d3737e", ""), rows.get(1));
        }
    }

    @Test
    public void addArchivalCollectionMappingsWithoutPidTest() throws Exception {
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
            assertIterableEquals(Arrays.asList("dmrecord:604", "", "604"), rows.get(0));
            assertIterableEquals(Arrays.asList("dmrecord:607", "", "607"), rows.get(1));
        }
    }

    @Test
    public void addArchivalCollectionMappingsWithoutForceFlagTest() throws Exception {
        solrResponseWithPid();
        writeCsv(mappingBody("aid:40126,bdbd99af-36a5-4bab-9785-e3a802d3737e,"));

        var options = new DestinationMappingOptions();
        options.setFieldName("dmrecord");

        Exception exception = assertThrows(StateAlreadyExistsException.class, () -> {
            service.addArchivalCollectionMappings(options);
        });
        String expectedMessage = "Cannot create destinations, a file already exists. Use the force flag to overwrite.";
        String actualMessage = exception.getMessage();

        assertTrue(actualMessage.contains(expectedMessage));
    }

    @Test
    public void addArchivalCollectionMappingsWithForceFlagTest() throws Exception {
        solrResponseWithPid();
        Path destinationMappingsPath = project.getDestinationMappingsPath();
        writeCsv(mappingBody("aid:40126,2720138a-a1ab-4b73-b7bc-6aabe75620c0,"));

        var options = new DestinationMappingOptions();
        options.setFieldName("dmrecord");
        options.setForce(true);

        service.addArchivalCollectionMappings(options);

        try (
                Reader reader = Files.newBufferedReader(destinationMappingsPath);
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                        .withFirstRecordAsHeader()
                        .withHeader(FieldUrlAssessmentService.URL_CSV_HEADERS)
                        .withTrim());
        ) {
            List<CSVRecord> rows = csvParser.getRecords();
            assertIterableEquals(Arrays.asList("dmrecord:604", "", "604"), rows.get(0));
            assertIterableEquals(Arrays.asList("dmrecord:607", "bdbd99af-36a5-4bab-9785-e3a802d3737e", ""), rows.get(1));
        }
    }

    @Test
    public void addArchivalCollectionMappingsWithoutFieldOptionTest() throws Exception {
        var options = new DestinationMappingOptions();

        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            service.addArchivalCollectionMappings(options);
        });
        String expectedMessage = "Field option is empty";
        String actualMessage = exception.getMessage();

        assertTrue(actualMessage.contains(expectedMessage));
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
        testDocumentA.addField(ArchivalDestinationsService.PID_KEY, "bdbd99af-36a5-4bab-9785-e3a802d3737e");
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
