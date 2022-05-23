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
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.test.SipServiceHelper;
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

        var testHelper = new SipServiceHelper(project, tmpFolder.newFolder().toPath());
        testHelper.indexExportData("gilmer");

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
        stubUrls(200);
        service.validateUrls();

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
        service.validateUrls();

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
            assertEquals("y", rows.get(1).get(FieldUrlAssessmentService.ERROR_INDICATOR));
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
        service.validateUrls();

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
            assertEquals("y", rows.get(1).get(FieldUrlAssessmentService.ERROR_INDICATOR));
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
        for (CSVRecord row : rows) {
            assertEquals(correctValue, row.get(column));
        }
        assertEquals(5, rows.size());
    }
}
