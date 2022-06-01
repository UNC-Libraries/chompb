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

import java.io.IOException;
import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.jena.atlas.logging.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;

/**
 * Service for validating cdm field urls report
 * @author krwong, snluong
 */
public class FieldUrlAssessmentService {
    private static final Logger log = LoggerFactory.getLogger(FieldUrlAssessmentService.class);

    private MigrationProject project;
    private CdmFieldService cdmFieldService;
    private CdmIndexService indexService;

    // header order: field, url, successful, redirect, redirect url
    public static final String NICK_FIELD = "cdm_nick";
    public static final String URL = "url";
    public static final String SUCCESSFUL_INDICATOR = "successful?";
    public static final String REDIRECT_INDICATOR = "redirect?";
    public static final String REDIRECT_URL = "redirect url";
    public static final String[] URL_CSV_HEADERS = new String[] {
            NICK_FIELD, URL, SUCCESSFUL_INDICATOR, REDIRECT_INDICATOR, REDIRECT_URL };

    /**
     * Generates a List of FieldUrlEntries that have the CDM field and associated URLs as attributes
     */
    protected List<FieldUrlEntry> dbFieldAndUrls() throws IOException {
        cdmFieldService.validateFieldsFile(project);
        CdmFieldInfo fieldInfo = cdmFieldService.loadFieldsFromProject(project);
        List<String> exportFields = fieldInfo.listAllExportFields();

        List<FieldUrlEntry> fieldUrlEntries = new ArrayList<>();
        Connection conn = null;

        try {
            conn = indexService.openDbConnection();
            Statement stmt = conn.createStatement();
            for (String field : exportFields) {
                // skip the "find" field. may be expanded to include other skip fields in the future
                if ("find".equals(field)) {
                    continue;
                }
                ResultSet rs = stmt.executeQuery(" select distinct \"" + field
                        + "\" from " + CdmIndexService.TB_NAME
                        + " where \"" + field + "\" like " + "'%http%'");
                while (rs.next()) {
                    fieldUrlEntries.add(new FieldUrlEntry(field, extractUrl(rs.getString(1))));
                }
            }
        } catch (SQLException e) {
            throw new MigrationException("Failed to query DB for URLs", e);
        } finally {
            CdmIndexService.closeDbConnection(conn);
        }
        return fieldUrlEntries.stream().distinct().collect(Collectors.toList());
    }

    /**
     * Extracts url from the passed in CDM field value
     */
    private String extractUrl(String string) {
        String regex = "\\b((https?):" + "//[-a-zA-Z0-9+&@#/%?=" + "~_|!:,.;]*[-a-zA-Z0-9+"
                + "&@#/%=~_|])";
        Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(string);
        String extractUrl = null;
        if (matcher.find()) {
            extractUrl = matcher.group();
        }
        return extractUrl;
    }

    /**
     * Validate urls, list redirect urls, and write to csv file
     */
    public void generateReport() throws IOException {
        var fieldsAndUrls = dbFieldAndUrls();

        Path projPath = project.getProjectPath();
        String filename = project.getProjectProperties().getName() + "_field_urls.csv";
        BufferedWriter writer = Files.newBufferedWriter(projPath.resolve(filename));
        CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT
                .withHeader(URL_CSV_HEADERS));
        var httpClient = HttpClientBuilder.create().disableRedirectHandling().build();

        // row order: field, url, successful, redirect, redirect URL
        for (var entry : fieldsAndUrls) {
            String field = entry.fieldName;
            String url = entry.url;
            HttpGet getMethod;

            // this try/catch is for syntactically invalid URLs
            try {
                getMethod = new HttpGet(url);
            } catch (IllegalArgumentException e) {
                csvPrinter.printRecord(field, url, "n", "n", null);
                continue;
            }

            try (CloseableHttpResponse resp = httpClient.execute(getMethod)) {
                int status = resp.getStatusLine().getStatusCode();
                if (isSuccess(status)) {
                    csvPrinter.printRecord(field, url, "y", "n", null);
                } else if (status >= 300 && status < 400) {
                    var result = getStatusAndFinalRedirectUrl(url);
                    var successString = result[0];
                    var finalRedirectUrl = result[1];
                    csvPrinter.printRecord(field, url, successString, "y", finalRedirectUrl);
                } else if (isFailure(status)) {
                    csvPrinter.printRecord(field, url, "n", "n", null);
                } else {
                    throw new IOException("Unrecognized response status: " + status + " for " + url);
                }
            } catch (IOException e) {
                log.warn(e.getMessage(), e);
                // invalid URL will be logged as an error url
                csvPrinter.printRecord(field, url, "n", "n", null);
            }
        }
        csvPrinter.close();
    }

    /**
     * Returns an array where the first element is "y" or "no" depending on status code
     * second element is the final redirect url (or null if there isn't one)
     * @param url is the original URL to test
     * @return Object[]
     */
    private Object[] getStatusAndFinalRedirectUrl(String url) throws IOException {
        HttpClientContext context = HttpClientContext.create();
        HttpGet getMethod;
        getMethod = new HttpGet(url);

        try (
                var httpClient = HttpClientBuilder.create().build();
                CloseableHttpResponse resp = httpClient.execute(getMethod, context);
            ) {
            int status = resp.getStatusLine().getStatusCode();
            String successString = isSuccess(status) ? "y" : "n";

            // get very last redirect url
            var redirectURIs = context.getRedirectLocations();
            if (redirectURIs != null && !redirectURIs.isEmpty()) {
                var finalUrl = redirectURIs.get(redirectURIs.size() - 1).toString();
                return new Object[] {successString, finalUrl};
            }
            return new Object[] {successString, null};
        }
    }

    public void setProject(MigrationProject project) {
        this.project = project;
    }

    public void setCdmFieldService(CdmFieldService cdmFieldService) {
        this.cdmFieldService = cdmFieldService;
    }

    public void setIndexService(CdmIndexService indexService) {
        this.indexService = indexService;
    }

    private boolean isSuccess(int status) {
        return status >= 200 && status < 300;
    }

    private boolean isFailure(int status) {
        return status >= 400;
    }

    public class FieldUrlEntry {
        private String fieldName;
        private String url;

        public FieldUrlEntry(String fieldName, String url) {
            this.fieldName = fieldName;
            this.url = url;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FieldUrlEntry that = (FieldUrlEntry) o;
            return Objects.equals(fieldName, that.fieldName) && Objects.equals(url, that.url);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fieldName, url);
        }
    }
}
