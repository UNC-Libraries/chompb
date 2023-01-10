package edu.unc.lib.boxc.migration.cdm.services;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.permanentRedirect;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.SipServiceHelper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import com.github.tomakehurst.wiremock.junit5.WireMockExtension;

import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;

/**
 * @author krwong, snluong
 */
public class FieldUrlAssessmentServiceTest {
    private static final String PROJECT_NAME = "gilmer";

    @TempDir
    public Path tmpFolder;

//    @Rule
//    public WireMockRule wireMockRule = new WireMockRule(options().dynamicPort());
    @RegisterExtension
    static WireMockExtension wireMockRule = WireMockExtension.newInstance()
            .options(wireMockConfig().dynamicPort().dynamicHttpsPort())
            .build();

    private MigrationProject project;
    private FieldUrlAssessmentService service;
    private CdmFieldService fieldService;
    private CdmIndexService indexService;
    private String cdmBaseUrl;

    @BeforeEach
    public void setup() throws Exception {
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder.getRoot(), PROJECT_NAME, null, "user", CdmEnvironmentHelper.DEFAULT_ENV_ID);
        Files.createDirectories(project.getExportPath());

        var testHelper = new SipServiceHelper(project, tmpFolder);
        testHelper.indexExportData("gilmer");

        fieldService = new CdmFieldService();
        indexService = new CdmIndexService();
        indexService.setProject(project);
        indexService.setFieldService(fieldService);
        service = new FieldUrlAssessmentService();
        service.setProject(project);
        service.setCdmFieldService(fieldService);
        service.setIndexService(indexService);

        cdmBaseUrl = "http://localhost:" + wireMockRule.getPort();
        fieldService.setCdmBaseUri(cdmBaseUrl);
        addUrlsToDb();
    }

    @Test
    public void retrieveCdmUrlsTest() throws Exception {
        List<FieldUrlAssessmentService.FieldUrlEntry> fieldsAndUrls = service.dbFieldAndUrls();

        assertEquals(5,fieldsAndUrls.size());
    }

    @Test
    public void successfulUrlsTest() throws Exception {
        stubUrls(200);
        service.generateReport();

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
        service.generateReport();

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
        service.generateReport();

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
        service.generateReport();

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
        service.generateReport();

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
        service.generateReport();

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

            assertEquals(invalidUrl, rows.get(1).get(FieldUrlAssessmentService.URL));
        }
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
