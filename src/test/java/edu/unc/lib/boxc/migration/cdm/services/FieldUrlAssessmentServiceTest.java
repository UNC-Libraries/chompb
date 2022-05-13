/**
 * Copyright 2008 The University of North Carolina at Chapel Hill
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.unc.lib.boxc.migration.cdm.services;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.github.tomakehurst.wiremock.junit.WireMockRule;

import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;

/**
 * @author krwong, snluong
 */
public class FieldUrlAssessmentServiceTest {
    private static final String PROJECT_NAME = "gilmer";

    @Rule
    public final TemporaryFolder tmpFolder = new TemporaryFolder();

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(options().dynamicPort());

    private MigrationProject project;
    private FieldUrlAssessmentService service;
    private CdmFieldService fieldService;
    private CdmIndexService indexService;
    private String cdmBaseUrl;

    @Before
    public void setup() throws Exception {
        tmpFolder.create();
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder.getRoot().toPath(), PROJECT_NAME, null, "user");
        Files.createDirectories(project.getExportPath());

        fieldService = new CdmFieldService();
        indexService = new CdmIndexService();
        indexService.setProject(project);
        indexService.setFieldService(fieldService);
        service = new FieldUrlAssessmentService();
        service.setProject(project);
        service.setCdmFieldService(fieldService);
        service.setIndexService(indexService);

        cdmBaseUrl = "http://localhost:" + wireMockRule.port();
        fieldService.setCdmBaseUri(cdmBaseUrl);
    }

    @Test
    public void retrieveCdmUrlsTest() throws Exception {
        indexExportSamples();
        addUrlsToDb();
        List<FieldUrlAssessmentService.FieldUrlEntry> fieldsAndUrls = service.dbFieldAndUrls(project);

        assertEquals(5,fieldsAndUrls.size());
    }

    @Test
    public void successfulUrlsTest() throws Exception {
        stubUrls(200);
        indexExportSamples();
        addUrlsToDb();

        service.validateUrls();

        try (
                Reader reader = Files.newBufferedReader(project.getProjectPath().resolve("gilmer_field_urls.csv"));
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                        .withFirstRecordAsHeader()
                        .withHeader(FieldUrlAssessmentService.URL_CSV_HEADERS)
                        .withTrim());
        ) {
            List<CSVRecord> rows = csvParser.getRecords();
            assertUrlsInCsvAreCorrect(rows);
            assertColumnValuesAreCorrect(rows, FieldUrlAssessmentService.ERROR_INDICATOR, "n");
            assertColumnValuesAreCorrect(rows, FieldUrlAssessmentService.SUCCESSFUL_INDICATOR, "y");
            assertColumnValuesAreCorrect(rows, FieldUrlAssessmentService.REDIRECT_INDICATOR, "n");
            assertColumnValuesAreCorrect(rows, FieldUrlAssessmentService.REDIRECT_URL, "");
        }
    }

    @Test
    public void redirectUrlsTest() throws Exception {
        stubRedirectUrls();

        indexExportSamples();
        addUrlsToDb();
        service.validateUrls();

        try (
                Reader reader = Files.newBufferedReader(project.getProjectPath().resolve("gilmer_field_urls.csv"));
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                        .withFirstRecordAsHeader()
                        .withHeader(FieldUrlAssessmentService.URL_CSV_HEADERS)
                        .withTrim());
        ) {
            List<CSVRecord> rows = csvParser.getRecords();
            assertUrlsInCsvAreCorrect(rows);
            assertColumnValuesAreCorrect(rows, FieldUrlAssessmentService.ERROR_INDICATOR, "n");
            assertColumnValuesAreCorrect(rows, FieldUrlAssessmentService.SUCCESSFUL_INDICATOR, "y");
            assertColumnValuesAreCorrect(rows, FieldUrlAssessmentService.REDIRECT_INDICATOR, "y");
            assertColumnValuesAreCorrect(rows, FieldUrlAssessmentService.REDIRECT_URL,
                    cdmBaseUrl + "/redirect_url");
        }
    }

    @Test
    public void errorUrlsTest() throws Exception {
        stubUrls(400);
        indexExportSamples();
        addUrlsToDb();
        service.validateUrls();

        try (
                Reader reader = Files.newBufferedReader(project.getProjectPath().resolve("gilmer_field_urls.csv"));
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                        .withFirstRecordAsHeader()
                        .withHeader(FieldUrlAssessmentService.URL_CSV_HEADERS)
                        .withTrim());
        ) {
            List<CSVRecord> rows = csvParser.getRecords();
            assertUrlsInCsvAreCorrect(rows);
            assertColumnValuesAreCorrect(rows, FieldUrlAssessmentService.ERROR_INDICATOR, "y");
            assertColumnValuesAreCorrect(rows, FieldUrlAssessmentService.SUCCESSFUL_INDICATOR, "n");
            assertColumnValuesAreCorrect(rows, FieldUrlAssessmentService.REDIRECT_INDICATOR, "n");
            assertColumnValuesAreCorrect(rows, FieldUrlAssessmentService.REDIRECT_URL, "");
        }
    }

    @Test
    public void regenerateCsv() throws Exception {
        indexExportSamples();

        service.validateUrls();

        Path projPath = project.getProjectPath();
        Path newPath = projPath.resolve("gilmer_field_urls.csv");

        assertTrue(Files.exists(newPath));
    }

    private void indexExportSamples() throws Exception {
        Files.copy(Paths.get("src/test/resources/sample_exports/export_1.xml"),
                project.getExportPath().resolve("export_1.xml"));
        Files.copy(Paths.get("src/test/resources/sample_exports/export_2.xml"),
                project.getExportPath().resolve("export_2.xml"));
        Files.copy(Paths.get("src/test/resources/gilmer_fields.csv"), project.getFieldsPath());

        project.getProjectProperties().setExportedDate(Instant.now());
        indexService.createDatabase(false);
        indexService.indexAll();
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
        stubFor(get(urlEqualTo( "/redirect_url"))
                .willReturn(aResponse()
                        .withStatus(200)));
        stubFor(get(urlEqualTo( "/00276/"))
                .willReturn(aResponse()
                        .withStatus(300)
                        .withHeader("Location", cdmBaseUrl + "/redirect_url")));
        stubFor(get(urlEqualTo( "/new_url_description"))
                .willReturn(aResponse()
                        .withStatus(300)
                        .withHeader("Location", cdmBaseUrl + "/redirect_url")));
        stubFor(get(urlEqualTo( "/new_url_notes"))
                .willReturn(aResponse()
                        .withStatus(300)
                        .withHeader("Location", cdmBaseUrl + "/redirect_url")));
        stubFor(get(urlEqualTo( "/new_url_caption"))
                .willReturn(aResponse()
                        .withStatus(300)
                        .withHeader("Location", cdmBaseUrl + "/redirect_url")));
        stubFor(get(urlEqualTo( "/new_url_caption_again"))
                .willReturn(aResponse()
                        .withStatus(300)
                        .withHeader("Location", cdmBaseUrl + "/redirect_url")));
    }

    private void addUrlsToDb() throws SQLException {
        Connection conn = indexService.openDbConnection();
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("UPDATE " + CdmIndexService.TB_NAME + " SET descri = descri || '" +
                cdmBaseUrl + "/new_url_description' WHERE cdmid = 25");
        stmt.executeUpdate("UPDATE " + CdmIndexService.TB_NAME + " SET descri = descri || '" +
                cdmBaseUrl + "/new_url_description' WHERE cdmid = 26");
        stmt.executeUpdate("UPDATE " + CdmIndexService.TB_NAME + " SET notes = notes || '" +
                cdmBaseUrl + "/new_url_notes' WHERE cdmid = 28");
        stmt.executeUpdate("UPDATE " + CdmIndexService.TB_NAME + " SET captio = captio || '" +
                cdmBaseUrl + "/new_url_caption' WHERE cdmid = 25");
        stmt.executeUpdate("UPDATE " + CdmIndexService.TB_NAME + " SET captio = captio || '" +
                cdmBaseUrl + "/new_url_caption_again' WHERE cdmid = 26");
        stmt.executeUpdate("UPDATE " + CdmIndexService.TB_NAME + " SET source = 'Jeremy Francis Gilmer Papers; " +
                cdmBaseUrl + "/00276/'");
        CdmIndexService.closeDbConnection(conn);
    }

    private void assertUrlsInCsvAreCorrect(List<CSVRecord> rows) {
        var column = FieldUrlAssessmentService.URL;
        assertEquals(cdmBaseUrl + "/new_url_description", rows.get(0).get(column));
        assertEquals(cdmBaseUrl + "/new_url_caption", rows.get(1).get(column));
        assertEquals(cdmBaseUrl + "/new_url_caption_again", rows.get(2).get(column));
        assertEquals(cdmBaseUrl + "/new_url_notes", rows.get(3).get(column));
        assertEquals(cdmBaseUrl + "/00276/", rows.get(4).get(column));
        assertEquals(5, rows.size());
    }

    private void assertColumnValuesAreCorrect(List<CSVRecord> rows, String column, String correctValue) {
        assertEquals(correctValue, rows.get(0).get(column));
        assertEquals(correctValue, rows.get(1).get(column));
        assertEquals(correctValue, rows.get(2).get(column));
        assertEquals(correctValue, rows.get(3).get(column));
        assertEquals(correctValue, rows.get(4).get(column));
        assertEquals(5, rows.size());
    }
}
