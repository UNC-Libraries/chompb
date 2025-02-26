package edu.unc.lib.boxc.migration.cdm.services;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.permanentRedirect;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;

import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.test.BxcEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.SipServiceHelper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;

import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;

/**
 * @author krwong, snluong
 */
@WireMockTest
public class FieldUrlAssessmentServiceTest {
    private static final String PROJECT_NAME = "gilmer";

    @TempDir
    public Path tmpFolder;

    private MigrationProject project;
    private FieldUrlAssessmentService service;
    private CdmFieldService fieldService;
    private CdmIndexService indexService;
    private SipServiceHelper testHelper;
    private String cdmBaseUrl;

    @BeforeEach
    public void setup(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        project = MigrationProjectFactory.createCdmMigrationProject(
                tmpFolder, PROJECT_NAME, null, "user",
                CdmEnvironmentHelper.DEFAULT_ENV_ID, BxcEnvironmentHelper.DEFAULT_ENV_ID);
        Files.createDirectories(project.getExportPath());

        testHelper = new SipServiceHelper(project, tmpFolder);
        testHelper.indexExportData("gilmer");

        fieldService = new CdmFieldService();
        indexService = new CdmIndexService();
        indexService.setProject(project);
        indexService.setFieldService(fieldService);
        service = new FieldUrlAssessmentService();
        service.setProject(project);
        service.setCdmFieldService(fieldService);
        service.setIndexService(indexService);

        cdmBaseUrl = "http://localhost:" + wmRuntimeInfo.getHttpPort();
        fieldService.setCdmBaseUri(cdmBaseUrl);
        addUrlsToDb();
    }

    @Test
    public void retrieveCdmUrlsTest() throws Exception {
        List<FieldUrlAssessmentService.FieldUrlEntry> fieldsAndUrls = service.dbFieldAndUrls(false);

        assertEquals(5,fieldsAndUrls.size());
    }

    @Test
    public void successfulUrlsTest() throws Exception {
        stubUrls(200);
        service.generateReport(false);

        try (
                Reader reader = Files.newBufferedReader(project.getProjectPath().resolve("gilmer_field_urls.csv"));
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                        .withFirstRecordAsHeader()
                        .withHeader(FieldUrlAssessmentService.URL_CSV_HEADERS)
                        .withTrim());
        ) {
            List<CSVRecord> rows = csvParser.getRecords();
            assertInfoInCsvAreCorrect(rows);
            assertColumnValuesAreCorrect(rows, FieldUrlAssessmentService.SUCCESSFUL_INDICATOR, "y");
            assertColumnValuesAreCorrect(rows, FieldUrlAssessmentService.REDIRECT_INDICATOR, "n");
            assertColumnValuesAreCorrect(rows, FieldUrlAssessmentService.REDIRECT_URL, "");
        }
    }

    @Test
    public void redirectUrlsTest() throws Exception {
        stubRedirectUrls();
        service.generateReport(false);

        try (
                Reader reader = Files.newBufferedReader(project.getProjectPath().resolve("gilmer_field_urls.csv"));
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                        .withFirstRecordAsHeader()
                        .withHeader(FieldUrlAssessmentService.URL_CSV_HEADERS)
                        .withTrim());
        ) {
            List<CSVRecord> rows = csvParser.getRecords();
            assertInfoInCsvAreCorrect(rows);
            assertColumnValuesAreCorrect(rows, FieldUrlAssessmentService.SUCCESSFUL_INDICATOR, "y");
            assertColumnValuesAreCorrect(rows, FieldUrlAssessmentService.REDIRECT_INDICATOR, "y");
            assertColumnValuesAreCorrect(rows, FieldUrlAssessmentService.REDIRECT_URL,
                    cdmBaseUrl + "/final_redirect_url");
        }
    }

    @Test
    public void errorUrlsTest() throws Exception {
        stubUrls(400);
        service.generateReport(false);

        try (
                Reader reader = Files.newBufferedReader(project.getProjectPath().resolve("gilmer_field_urls.csv"));
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                        .withFirstRecordAsHeader()
                        .withHeader(FieldUrlAssessmentService.URL_CSV_HEADERS)
                        .withTrim());
        ) {
            List<CSVRecord> rows = csvParser.getRecords();
            assertInfoInCsvAreCorrect(rows);
            assertColumnValuesAreCorrect(rows, FieldUrlAssessmentService.SUCCESSFUL_INDICATOR, "n");
            assertColumnValuesAreCorrect(rows, FieldUrlAssessmentService.REDIRECT_INDICATOR, "n");
            assertColumnValuesAreCorrect(rows, FieldUrlAssessmentService.REDIRECT_URL, "");
        }
    }

    @Test
    public void regenerateCsv() throws Exception {
        stubUrls(HttpStatus.SC_OK);
        service.generateReport(false);

        Path projPath = project.getProjectPath();
        Path newPath = projPath.resolve("gilmer_field_urls.csv");

        assertTrue(Files.exists(newPath));
    }

    @Test
    public void urlWithSpaceTest() throws Exception {
        var wrongUrl = cdmBaseUrl + "/wrong";
        addProblematicUrlToDb(wrongUrl + " .html");
        stubUrls(200);
        stubFor(get(urlEqualTo( "/wrong"))
                .willReturn(aResponse()
                        .withStatus(400)));
        service.generateReport(false);

        try (
                Reader reader = Files.newBufferedReader(project.getProjectPath().resolve("gilmer_field_urls.csv"));
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                        .withFirstRecordAsHeader()
                        .withHeader(FieldUrlAssessmentService.URL_CSV_HEADERS)
                        .withTrim());
        ) {
            List<CSVRecord> rows = csvParser.getRecords();
            assertEquals("y", rows.get(0).get(FieldUrlAssessmentService.SUCCESSFUL_INDICATOR));
            // the following one is the wrong url
            assertEquals("n", rows.get(1).get(FieldUrlAssessmentService.SUCCESSFUL_INDICATOR));
            assertEquals("y", rows.get(2).get(FieldUrlAssessmentService.SUCCESSFUL_INDICATOR));
            assertEquals("y", rows.get(3).get(FieldUrlAssessmentService.SUCCESSFUL_INDICATOR));
            assertEquals("y", rows.get(4).get(FieldUrlAssessmentService.SUCCESSFUL_INDICATOR));
            assertEquals("y", rows.get(5).get(FieldUrlAssessmentService.SUCCESSFUL_INDICATOR));

            assertEquals(wrongUrl, rows.get(1).get(FieldUrlAssessmentService.URL));
        }
    }

    @Test
    public void invalidUrlTest() throws Exception {
        var invalidUrl = cdmBaseUrl + "&";
        addProblematicUrlToDb(invalidUrl);
        stubUrls(200);
        service.generateReport(false);

        try (
                Reader reader = Files.newBufferedReader(project.getProjectPath().resolve("gilmer_field_urls.csv"));
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                        .withFirstRecordAsHeader()
                        .withHeader(FieldUrlAssessmentService.URL_CSV_HEADERS)
                        .withTrim());
        ) {
            List<CSVRecord> rows = csvParser.getRecords();
            assertEquals("y", rows.get(0).get(FieldUrlAssessmentService.SUCCESSFUL_INDICATOR));
            assertEquals("n", rows.get(1).get(FieldUrlAssessmentService.SUCCESSFUL_INDICATOR));
            assertEquals("y", rows.get(2).get(FieldUrlAssessmentService.SUCCESSFUL_INDICATOR));
            assertEquals("y", rows.get(3).get(FieldUrlAssessmentService.SUCCESSFUL_INDICATOR));
            assertEquals("y", rows.get(4).get(FieldUrlAssessmentService.SUCCESSFUL_INDICATOR));
            assertEquals("y", rows.get(5).get(FieldUrlAssessmentService.SUCCESSFUL_INDICATOR));
            assertEquals(6, rows.size());

            assertEquals(invalidUrl, rows.get(1).get(FieldUrlAssessmentService.URL));
        }
    }

    @Test
    public void urlWithSpaceAfterHttpTest() throws Exception {
        var invalidUrl = "http ://example.com";
        addProblematicUrlToDb(invalidUrl);
        stubUrls(200);
        service.generateReport(false);

        try (
                Reader reader = Files.newBufferedReader(project.getProjectPath().resolve("gilmer_field_urls.csv"));
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                        .withFirstRecordAsHeader()
                        .withHeader(FieldUrlAssessmentService.URL_CSV_HEADERS)
                        .withTrim());
        ) {
            List<CSVRecord> rows = csvParser.getRecords();
            // the report should contain all the regular URLs, but should not contain the invalid URL
            var urlValues = listUrlValues(rows);
            assertIterableEquals(List.of(
                    cdmBaseUrl + "/new_url_description",
                    cdmBaseUrl + "/new_url_caption",
                    cdmBaseUrl + "/new_url_caption_again",
                    cdmBaseUrl + "/new_url_notes",
                    cdmBaseUrl + "/00276/"),
                    urlValues);
        }
    }

    @Test
    public void urlWithSpaceAfterProtocolTest() throws Exception {
        var invalidUrl = "http:// example.com";
        addProblematicUrlToDb(invalidUrl);
        stubUrls(200);
        service.generateReport(false);

        try (
                Reader reader = Files.newBufferedReader(project.getProjectPath().resolve("gilmer_field_urls.csv"));
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                        .withFirstRecordAsHeader()
                        .withHeader(FieldUrlAssessmentService.URL_CSV_HEADERS)
                        .withTrim());
        ) {
            List<CSVRecord> rows = csvParser.getRecords();
            // the report should contain all the regular URLs, but should not contain the invalid URL
            var urlValues = listUrlValues(rows);
            assertIterableEquals(List.of(
                            cdmBaseUrl + "/new_url_description",
                            cdmBaseUrl + "/new_url_caption",
                            cdmBaseUrl + "/new_url_caption_again",
                            cdmBaseUrl + "/new_url_notes",
                            cdmBaseUrl + "/00276/"),
                    urlValues);
        }
    }

    @Test
    public void urlWithTinyDomainTest() throws Exception {
        var invalidUrl = "https://e xample.com";
        addProblematicUrlToDb(invalidUrl);
        stubUrls(200);
        service.generateReport(false);

        try (
                Reader reader = Files.newBufferedReader(project.getProjectPath().resolve("gilmer_field_urls.csv"));
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                        .withFirstRecordAsHeader()
                        .withHeader(FieldUrlAssessmentService.URL_CSV_HEADERS)
                        .withTrim());
        ) {
            List<CSVRecord> rows = csvParser.getRecords();
            // the report should contain all the regular URLs, but should not contain the invalid URL
            var urlValues = listUrlValues(rows);
            assertIterableEquals(List.of(
                            cdmBaseUrl + "/new_url_description",
                            cdmBaseUrl + "/new_url_caption",
                            cdmBaseUrl + "/new_url_caption_again",
                            cdmBaseUrl + "/new_url_notes",
                            cdmBaseUrl + "/00276/"),
                    urlValues);
        }
    }

    @Test
    public void retrieveFindingAidUrlsTest() throws Exception {
        addFindingAidUrlsToDb();
        List<FieldUrlAssessmentService.FieldUrlEntry> fieldsAndUrls = service.dbFieldAndUrls(true);

        assertEquals(1,fieldsAndUrls.size());
    }

    @Test
    public void successfulFindingAidUrlsTest() throws Exception {
        addFindingAidUrlsToDb();
        stubFindingAidUrls(200);
        service.generateReport(true);

        try (
                Reader reader = Files.newBufferedReader(project.getProjectPath()
                        .resolve("gilmer_finding_aid_urls.csv"));
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                        .withFirstRecordAsHeader()
                        .withHeader(FieldUrlAssessmentService.URL_CSV_HEADERS)
                        .withTrim());
        ) {
            List<CSVRecord> rows = csvParser.getRecords();
            var urlValues = listUrlValues(rows);
            assertIterableEquals(List.of("http://finding-aids.lib.unc.edu/03883/"),
                    urlValues);
        }
    }

    private List<String> listUrlValues(List<CSVRecord> rows) {
        return rows.stream().map(row -> row.get(FieldUrlAssessmentService.URL)).collect(Collectors.toList());
    }

    private void stubUrls(int statusCode) {
        stubFor(get(urlEqualTo( "/00276/"))
                .willReturn(aResponse()
                        .withStatus(statusCode)));
        stubFor(get(urlEqualTo( "/new_url_description"))
                .willReturn(aResponse()
                        .withStatus(statusCode)));
        stubFor(get(urlEqualTo( "/new_url_notes"))
                .willReturn(aResponse()
                        .withStatus(statusCode)));
        stubFor(get(urlEqualTo( "/new_url_caption"))
                .willReturn(aResponse()
                        .withStatus(statusCode)));
        stubFor(get(urlEqualTo( "/new_url_caption_again"))
                .willReturn(aResponse()
                        .withStatus(statusCode)));
    }

    private void stubRedirectUrls() {
        stubFor(get(urlEqualTo("/00276/"))
                .willReturn(permanentRedirect(cdmBaseUrl + "/new_redirect_url")));
        stubFor(get(urlEqualTo("/new_url_notes"))
                .willReturn(permanentRedirect(cdmBaseUrl + "/new_redirect_url")));
        stubFor(get(urlEqualTo("/new_url_description"))
                .willReturn(permanentRedirect(cdmBaseUrl + "/new_redirect_url")));
        stubFor(get(urlEqualTo("/new_url_caption"))
                .willReturn(permanentRedirect(cdmBaseUrl + "/new_redirect_url")));
        stubFor(get(urlEqualTo("/new_url_caption_again"))
                .willReturn(permanentRedirect(cdmBaseUrl + "/new_redirect_url")));
        stubFor(get(urlEqualTo("/new_redirect_url"))
                .willReturn(permanentRedirect(cdmBaseUrl + "/final_redirect_url")));
        stubFor(get(urlEqualTo( "/final_redirect_url"))
                .willReturn(aResponse()
                        .withStatus(HttpStatus.SC_OK)));
    }

    private void addUrlsToDb() throws SQLException {
        Connection conn = indexService.openDbConnection();
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("UPDATE " + CdmIndexService.TB_NAME + " SET descri = descri || '" +
                cdmBaseUrl + "/new_url_description' WHERE " + CdmFieldInfo.CDM_ID + " = 25");
        stmt.executeUpdate("UPDATE " + CdmIndexService.TB_NAME + " SET descri = descri || '" +
                cdmBaseUrl + "/new_url_description' WHERE " + CdmFieldInfo.CDM_ID + " = 26");
        stmt.executeUpdate("UPDATE " + CdmIndexService.TB_NAME + " SET notes = notes || '" +
                cdmBaseUrl + "/new_url_notes' WHERE " + CdmFieldInfo.CDM_ID + "= 28");
        stmt.executeUpdate("UPDATE " + CdmIndexService.TB_NAME + " SET captio = captio || '" +
                cdmBaseUrl + "/new_url_caption' WHERE " + CdmFieldInfo.CDM_ID + " = 25");
        stmt.executeUpdate("UPDATE " + CdmIndexService.TB_NAME + " SET captio = captio || '" +
                cdmBaseUrl + "/new_url_caption_again' WHERE " + CdmFieldInfo.CDM_ID + " = 26");
        stmt.executeUpdate("UPDATE " + CdmIndexService.TB_NAME + " SET source = 'Jeremy Francis Gilmer Papers; " +
                cdmBaseUrl + "/00276/'");
        CdmIndexService.closeDbConnection(conn);
    }

    private void addProblematicUrlToDb(String url) throws SQLException {
        Connection conn = indexService.openDbConnection();
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("UPDATE " + CdmIndexService.TB_NAME + " SET descri = descri || '" +
                url + "' WHERE " + CdmFieldInfo.CDM_ID + " = 100");
        CdmIndexService.closeDbConnection(conn);
    }

    private void stubFindingAidUrls(int statusCode) {
        stubFor(get(urlEqualTo( "http://finding-aids.lib.unc.edu/03883/"))
                .willReturn(aResponse()
                        .withStatus(statusCode)));
    }

    private void addFindingAidUrlsToDb() throws SQLException {
        Connection conn = indexService.openDbConnection();
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("UPDATE " + CdmIndexService.TB_NAME + " SET descri = descri || " +
                " 'http://finding-aids.lib.unc.edu/03883/' WHERE " + CdmFieldInfo.CDM_ID + " = 100");
        CdmIndexService.closeDbConnection(conn);
    }

    private void assertInfoInCsvAreCorrect(List<CSVRecord> rows) {
        var urlColumn = FieldUrlAssessmentService.URL;
        assertEquals(cdmBaseUrl + "/new_url_description", rows.get(0).get(urlColumn));
        assertEquals(cdmBaseUrl + "/new_url_caption", rows.get(1).get(urlColumn));
        assertEquals(cdmBaseUrl + "/new_url_caption_again", rows.get(2).get(urlColumn));
        assertEquals(cdmBaseUrl + "/new_url_notes", rows.get(3).get(urlColumn));
        assertEquals(cdmBaseUrl + "/00276/", rows.get(4).get(urlColumn));

        var nickColumn = FieldUrlAssessmentService.NICK_FIELD;
        assertEquals("descri", rows.get(0).get(nickColumn));
        assertEquals("captio", rows.get(1).get(nickColumn));
        assertEquals("captio", rows.get(2).get(nickColumn));
        assertEquals("notes", rows.get(3).get(nickColumn));
        assertEquals("source", rows.get(4).get(nickColumn));
        assertEquals(5, rows.size());
    }

    private void assertColumnValuesAreCorrect(List<CSVRecord> rows, String column, String correctValue) {
        for (CSVRecord row : rows) {
            assertEquals(correctValue, row.get(column));
        }
        assertEquals(5, rows.size());
    }
}
