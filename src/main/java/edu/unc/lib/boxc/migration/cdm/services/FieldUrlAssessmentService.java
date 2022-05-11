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
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;

/**
 * Service for validating cdm field urls report
 * @author krwong
 */
public class FieldUrlAssessmentService {
    private static final Logger log = LoggerFactory.getLogger(FieldUrlAssessmentService.class);

    private MigrationProject project;
    private CdmFieldService cdmFieldService;
    private CdmIndexService indexService;

    // header: field, url, error, successful, redirect, redirect url
    public static final String NICK_FIELD = "cdm_nick";
    public static final String URL = "url";
    public static final String ERROR_INDICATOR = "error?";
    public static final String SUCCESSFUL_INDICATOR = "successful?";
    public static final String REDIRECT_INDICATOR = "redirect?";
    public static final String REDIRECT_URL = "redirect url";
    public static final String[] URL_CSV_HEADERS = new String[] {
            NICK_FIELD, URL, ERROR_INDICATOR, SUCCESSFUL_INDICATOR, REDIRECT_INDICATOR, REDIRECT_URL };

    /**
     * List all fields and urls that were exported
     * @throws IOException
     */
    //TODO: have this method return object with the field and url instead of list of urls
    public List<String> dbFieldUrls(MigrationProject project) throws IOException {
        indexService.setProject(project);
        cdmFieldService.validateFieldsFile(project);
        CdmFieldInfo fieldInfo = cdmFieldService.loadFieldsFromProject(project);
        List<String> exportFields = fieldInfo.listAllExportFields();

        List<String> listFieldUrls = new ArrayList<>();
        Connection conn = null;

        try {
            conn = indexService.openDbConnection();
            Statement stmt = conn.createStatement();
            for (String field : exportFields) {
                ResultSet rs = stmt.executeQuery(" select distinct " + field
                        + " from " + CdmIndexService.TB_NAME
                        + " where " + field + " like " + "'%http%'");
                while (rs.next()) {
                    listFieldUrls.add(rs.getString(1));
                }
            }
        } catch (SQLException e) {
            throw new MigrationException("Failed to generate field urls", e);
        } finally {
            CdmIndexService.closeDbConnection(conn);
        }
        return listFieldUrls;
    }

    public Map<String, String> dbFieldAndUrls(MigrationProject project) throws IOException {
        indexService.setProject(project);
        cdmFieldService.validateFieldsFile(project);
        CdmFieldInfo fieldInfo = cdmFieldService.loadFieldsFromProject(project);
        List<String> exportFields = fieldInfo.listAllExportFields();

        Map<String, String> fieldsAndUrls = new HashMap<>();
        Connection conn = null;

        try {
            conn = indexService.openDbConnection();
            Statement stmt = conn.createStatement();
            for (String field : exportFields) {
                if ("find".equals(field)) {
                    continue;
                }
                ResultSet rs = stmt.executeQuery(" select distinct " + field
                        + " from " + CdmIndexService.TB_NAME
                        + " where " + field + " like " + "'%http%'");
                while (rs.next()) {
                    fieldsAndUrls.put(field, extractUrls(rs.getString(1)));
                }
            }
        } catch (SQLException e) {
            throw new MigrationException("Failed to generate fields with urls", e);
        } finally {
            CdmIndexService.closeDbConnection(conn);
        }
        return fieldsAndUrls;
    }

    /**
     * Extracts urls from each field
     * @throws IOException
     */
    public String extractUrls(String string) throws IOException {
        String regex = "\\b((?:https?|ftp|file):" + "//[-a-zA-Z0-9+&@#/%?=" + "~_|!:, .;]*[-a-zA-Z0-9+"
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
     * @throws IOException
     */
    public void validateUrls() throws IOException {
        //List<String> listUrls = dbFieldUrls(project);
        var fieldsAndUrls = dbFieldAndUrls(project);

        Path projPath = project.getProjectPath();
        String filename = project.getProjectProperties().getName() + "_field_urls.csv";
        BufferedWriter writer = Files.newBufferedWriter(projPath.resolve(filename));
//        BufferedWriter writer = new BufferedWriter(new FileWriter(filename));
        CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT
                .withHeader(URL_CSV_HEADERS));
        var httpClient = HttpClients.createDefault();

        // row order: field, url, error, successful, redirect, redirect URL
        for(Map.Entry<String, String> entry : fieldsAndUrls.entrySet()) {
        //for (String urlField : listUrls) {
            //String url = extractUrls(urlField);
            String field = entry.getKey();
            String url = entry.getValue();
            HttpGet getMethod = new HttpGet(url);
            try (CloseableHttpResponse resp = httpClient.execute(getMethod)) {
                int status = resp.getStatusLine().getStatusCode();
                if (status >= 200 && status < 300) {
//                    String success = "n,y,n, ";
//                    csvPrinter.printRecord(url, success);
                    csvPrinter.printRecord(field, url, "n", "y", "n", null);
                } else if (status >= 300 && status < 400) {
                    String redirect = "n,y,y,";
                    String redirectUrl = resp.getFirstHeader("Location").getValue();
                    csvPrinter.printRecord(url, redirect, redirectUrl);
                } else if (status >= 400) {
                    String error = "y,n,n, ";
                    csvPrinter.printRecord(url, error);
                } else {
                    throw new IOException("");
                }
            } catch (IOException e) {
                log.error("Failed to retrieve url: {}", url, e);
            }
        }
        csvPrinter.close();
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
}