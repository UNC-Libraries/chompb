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

import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.util.DisplayProgressUtil;
import edu.unc.lib.boxc.migration.cdm.util.PostMigrationReportConstants;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.springframework.http.HttpStatus;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Reader;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * Service which verifies the Box-c URLs in the post migration report and updates the verified field
 *
 * @author bbpennel
 */
public class PostMigrationReportVerifier {
    private MigrationProject project;
    private CloseableHttpClient httpClient;
    private boolean showProgress;

    public VerificationOutcome verify() throws IOException {
        validateReport();

        var outcome = new VerificationOutcome();
        var totalRecords = countNumberOfRecords();
        // Read the report so that we can write out a new version of it with the 'verified' field filled in
        var updatedPath = makeTempReportPath();
        try (
            var csvParser = openCsvParser();
            var csvPrinter = openCsvPrinter(updatedPath);
        ) {
            long currentNum = 0;
            updateProgressDisplay(currentNum, totalRecords);

            for (CSVRecord originalRecord : csvParser) {
                var verified = originalRecord.get(PostMigrationReportConstants.VERIFIED_HEADER);

                var rowValues = originalRecord.toList();
                // 'verified' field is empty or was not previously successful, so request the boxc url
                if (!isStatusAcceptable(verified)) {
                    var boxcUrl = originalRecord.get(PostMigrationReportConstants.BXC_URL_HEADER);
                    var result = requestHttpResult(boxcUrl);
                    outcome.recordResult(result);
                    rowValues.set(PostMigrationReportConstants.VERIFIED_INDEX, result);
                }
                // Write the row out into the new version of the report
                csvPrinter.printRecord(rowValues);

                currentNum++;
                updateProgressDisplay(currentNum, totalRecords);
            }
        }
        // swap the updated report for the old version, delete old version
        Files.move(updatedPath, project.getPostMigrationReportPath(), StandardCopyOption.REPLACE_EXISTING);
        outcome.totalRecords = totalRecords;
        return outcome;
    }

    // Update progress display, if showing
    private void updateProgressDisplay(long current, long total) {
        if (showProgress) {
            DisplayProgressUtil.displayProgress(current, total);
        }
    }

    /**
     * Checks the given boxc url and returns OK if the URL was OK or FORBIDDEN, otherwise it returns
     * the text representation of the http status.
     * @param bxcUrl
     * @return
     * @throws IOException
     */
    private String requestHttpResult(String bxcUrl) throws IOException {
        var getRequest = new HttpGet(URI.create(bxcUrl));
        try (var resp = httpClient.execute(getRequest)) {
            var status = resp.getStatusLine().getStatusCode();
            if (status == HttpStatus.OK.value() || status == HttpStatus.FORBIDDEN.value()) {
                return HttpStatus.OK.name();
            }
            return HttpStatus.valueOf(status).name();
        }
    }

    private CSVParser openCsvParser() throws IOException {
        Reader reader = Files.newBufferedReader(project.getPostMigrationReportPath());
        return new CSVParser(reader, PostMigrationReportConstants.CSV_PARSER_FORMAT);
    }

    private Path makeTempReportPath() throws IOException {
        var mappingPath = project.getPostMigrationReportPath();
        var tempPath = mappingPath.getParent().resolve("~" + mappingPath.getFileName().toString() + "_new");
        // Clean up any leftover temp path
        Files.deleteIfExists(tempPath);
        return tempPath;
    }

    private CSVPrinter openCsvPrinter(Path reportPath) throws IOException {
        BufferedWriter writer = Files.newBufferedWriter(reportPath);
        return new CSVPrinter(writer, PostMigrationReportConstants.CSV_OUTPUT_FORMAT);
    }

    private void validateReport() {
        // Verify that the report exists
        if (Files.notExists(project.getPostMigrationReportPath())) {
            throw new InvalidProjectStateException("Post migration report has not been generated yet");
        }
    }

    // Calculate total number of records (lines - header)
    private long countNumberOfRecords() throws IOException {
        long numLines = Files.lines(project.getPostMigrationReportPath()).count();
        return numLines - 1;
    }

    public void setProject(MigrationProject project) {
        this.project = project;
    }

    public void setHttpClient(CloseableHttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public void setShowProgress(boolean showProgress) {
        this.showProgress = showProgress;
    }

    private static boolean isStatusAcceptable(String status) {
        return HttpStatus.OK.name().equals(status) || HttpStatus.FORBIDDEN.name().equals(status);
    }

    public static class VerificationOutcome {
        public long errorCount = 0;
        public long verifiedCount = 0;
        public long totalRecords = 0;

        protected void recordResult(String result) {
            if (!isStatusAcceptable(result)) {
                errorCount++;
            }
            verifiedCount++;
        }

        public boolean hasErrors() {
            return errorCount > 0;
        }
    }
}
