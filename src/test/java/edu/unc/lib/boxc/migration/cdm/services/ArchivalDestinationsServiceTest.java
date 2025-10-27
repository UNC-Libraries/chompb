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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.MockitoAnnotations.openMocks;
import static org.mockito.Mockito.when;

public class ArchivalDestinationsServiceTest {
    private static final String PROJECT_NAME = "proj";
    private static final String DEST_UUID = "bfe93126-849a-43a5-b9d9-391e18ffacc6";
    private static final String DEST_UUID2 = "8ae56bbc-400e-496d-af4b-3c585e20dba1";
    private static final String DEST_UUID3 = "bdbd99af-36a5-4bab-9785-e3a802d3737e";
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
                tmpFolder, PROJECT_NAME, null, "user", CdmEnvironmentHelper.DEFAULT_ENV_ID,
                BxcEnvironmentHelper.DEFAULT_ENV_ID, MigrationProject.PROJECT_SOURCE_CDM);
        testHelper = new SipServiceHelper(project, tmpFolder);
        service = new ArchivalDestinationsService();
        service.setProject(project);
        service.setIndexService(testHelper.getCdmIndexService());
        service.setDestinationsService(testHelper.getDestinationsService());
        service.setSolrServerUrl(SOLR_URL);
        service.setSolr(solrClient);
    }

    @AfterEach
    void closeService() throws Exception {
        closeable.close();
    }

    @Test
    public void generateCollectionNumbersListKeepsakesTest() throws Exception {
        testHelper.indexExportData("mini_keepsakes");

        var options = new DestinationMappingOptions();
        options.setFieldName("dmrecord");

        var result = service.generateCollectionNumbersList(options);
        assertIterableEquals(Arrays.asList("216", "604", "607"), result);
    }

    @Test
    public void generateCollectionNumbersListGilmerTest() throws Exception {
        testHelper.indexExportData("grouped_gilmer");

        var options = new DestinationMappingOptions();
        options.setFieldName("groupa");

        var result = service.generateCollectionNumbersList(options);
        assertIterableEquals(Arrays.asList("group1", "group2", "group11"), result);
    }

    @Test
    public void generateCollectionNumbersToPidMappingNullPidKeepsakesTest() throws Exception {
        solrResponseWithoutPidKeepsakes();

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
    public void generateCollectionNumbersToPidMappingWithPidKeepsakesTest() throws Exception {
        solrResponseWithPidKeepsakes();

        Map<String, String> expected = new HashMap<>();
        expected.put("216", DEST_UUID);
        expected.put("604", null);
        expected.put("607", DEST_UUID2);

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
    public void generateCollectionNumbersToPidMappingWithPidGilmerTest() throws Exception {
        solrResponseWithPidGilmer();

        Map<String, String> expected = new HashMap<>();
        expected.put("group1", DEST_UUID);
        expected.put("group2", DEST_UUID2);
        expected.put("group11", DEST_UUID2);

        var options = new DestinationMappingOptions();
        options.setFieldName("groupa");

        var result = service.generateCollectionNumbersToPidMapping(options);
        assertEquals(expected, result);
        // not checking solrValues, because solrResponseWithPidGilmer doesn't mock solrQueryCaptor
    }

    @Test
    public void addArchivalCollectionMappingsWithPidKeepsakesTest() throws Exception {
        solrResponseWithPidKeepsakes();
        Path destinationMappingsPath = project.getDestinationMappingsPath();

        var options = new DestinationMappingOptions();
        options.setFieldName("dmrecord");

        service.addArchivalCollectionMappings(options);
        assertTrue(Files.exists(destinationMappingsPath));

        try (
                Reader reader = Files.newBufferedReader(destinationMappingsPath);
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                        .withFirstRecordAsHeader()
                        .withHeader(DestinationsInfo.CSV_HEADERS)
                        .withTrim());
        ) {
            List<CSVRecord> rows = csvParser.getRecords();
            assertIterableEquals(Arrays.asList("dmrecord:216", DEST_UUID, ""), rows.get(0));
            assertIterableEquals(Arrays.asList("dmrecord:604", "", "604"), rows.get(1));
            assertIterableEquals(Arrays.asList("dmrecord:607", DEST_UUID2, ""), rows.get(2));
        }
    }

    @Test
    public void addArchivalCollectionMappingsWithPidGilmerTest() throws Exception {
        solrResponseWithPidGilmer();
        Path destinationMappingsPath = project.getDestinationMappingsPath();

        var options = new DestinationMappingOptions();
        options.setFieldName("groupa");

        service.addArchivalCollectionMappings(options);
        assertTrue(Files.exists(destinationMappingsPath));

        try (
                Reader reader = Files.newBufferedReader(destinationMappingsPath);
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                        .withFirstRecordAsHeader()
                        .withHeader(DestinationsInfo.CSV_HEADERS)
                        .withTrim());
        ) {
            List<CSVRecord> rows = csvParser.getRecords();
            assertIterableEquals(Arrays.asList("groupa:group11", DEST_UUID2, ""), rows.get(0));
            assertIterableEquals(Arrays.asList("groupa:group2", DEST_UUID2, ""), rows.get(1));
            assertIterableEquals(Arrays.asList("groupa:group1", DEST_UUID, ""), rows.get(2));
        }
    }

    @Test
    public void addArchivalCollectionMappingsWithoutPidKeepsakesTest() throws Exception {
        solrResponseWithoutPidKeepsakes();
        Path destinationMappingsPath = project.getDestinationMappingsPath();

        var options = new DestinationMappingOptions();
        options.setFieldName("dmrecord");
        options.setDefaultCollection("001234");
        options.setDefaultDestination(DEST_UUID3);

        service.addArchivalCollectionMappings(options);

        try (
                Reader reader = Files.newBufferedReader(destinationMappingsPath);
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                        .withFirstRecordAsHeader()
                        .withHeader(DestinationsInfo.CSV_HEADERS)
                        .withTrim());
        ) {
            List<CSVRecord> rows = csvParser.getRecords();
            assertIterableEquals(Arrays.asList("dmrecord:216", "", "216"), rows.get(0));
            assertIterableEquals(Arrays.asList("dmrecord:604", "", "604"), rows.get(1));
            assertIterableEquals(Arrays.asList("dmrecord:607", "", "607"), rows.get(2));
            assertIterableEquals(Arrays.asList("default", DEST_UUID3, "001234"), rows.get(3));
        }
    }

    @Test
    public void addArchivalCollectionMappingsWithoutForceFlagKeepsakesTest() throws Exception {
        solrResponseWithPidKeepsakes();
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
    public void addArchivalCollectionMappingsWithForceFlagKeepsakesTest() throws Exception {
        solrResponseWithPidKeepsakes();
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
                        .withHeader(DestinationsInfo.CSV_HEADERS)
                        .withTrim());
        ) {
            List<CSVRecord> rows = csvParser.getRecords();
            assertIterableEquals(Arrays.asList("dmrecord:216", DEST_UUID, ""), rows.get(0));
            assertIterableEquals(Arrays.asList("dmrecord:604", "", "604"), rows.get(1));
            assertIterableEquals(Arrays.asList("dmrecord:607", DEST_UUID2, ""), rows.get(2));
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

    private void solrResponseWithoutPidKeepsakes() throws Exception {
        testHelper.indexExportData("mini_keepsakes");
        QueryResponse testResponse = new QueryResponse();
        testResponse.setResponse(new NamedList<>(Map.of("response", new SolrDocumentList())));
        when(solrClient.query(solrQueryCaptor.capture())).thenReturn(testResponse);
    }

    private void solrResponseWithPidKeepsakes() throws Exception {
        testHelper.indexExportData("mini_keepsakes");

        QueryResponse testResponse1 = new QueryResponse();
        SolrDocument testDocument1 = new SolrDocument();
        testDocument1.addField(ArchivalDestinationsService.PID_KEY, DEST_UUID);
        SolrDocumentList testList1 = new SolrDocumentList();
        testList1.add(testDocument1);
        testResponse1.setResponse(new NamedList<>(Map.of("response", testList1)));

        QueryResponse testResponse2 = new QueryResponse();
        testResponse2.setResponse(new NamedList<>(Map.of("response", new SolrDocumentList())));

        QueryResponse testResponse3 = new QueryResponse();
        SolrDocument testDocument3 = new SolrDocument();
        testDocument3.addField(ArchivalDestinationsService.PID_KEY, DEST_UUID2);
        SolrDocumentList testList3 = new SolrDocumentList();
        testList3.add(testDocument3);
        testResponse3.setResponse(new NamedList<>(Map.of("response", testList3)));

        when(solrClient.query(solrQueryCaptor.capture())).thenReturn(testResponse1).thenReturn(testResponse2)
                .thenReturn(testResponse3);
    }

    private void solrResponseWithPidGilmer() throws Exception {
        testHelper.indexExportData("grouped_gilmer");

        QueryResponse testResponse1 = new QueryResponse();
        SolrDocument testDocument1 = new SolrDocument();
        testDocument1.addField(ArchivalDestinationsService.PID_KEY, DEST_UUID);
        SolrDocumentList testList1 = new SolrDocumentList();
        testList1.add(testDocument1);
        testResponse1.setResponse(new NamedList<>(Map.of("response", testList1)));

        QueryResponse testResponse2 = new QueryResponse();
        SolrDocument testDocument2 = new SolrDocument();
        testDocument2.addField(ArchivalDestinationsService.PID_KEY, DEST_UUID2);
        SolrDocumentList testList2 = new SolrDocumentList();
        testList2.add(testDocument2);
        testResponse2.setResponse(new NamedList<>(Map.of("response", testList2)));

        when(solrClient.query(any())).thenAnswer(invocation -> {
            var query = invocation.getArgument(0, SolrQuery.class);
            var solrQ = query.get("q");
            if (solrQ.equals(ArchivalDestinationsService.COLLECTION_ID + ":group1")) {
                return testResponse1;
            } else {
                return testResponse2;
            }
        });
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
