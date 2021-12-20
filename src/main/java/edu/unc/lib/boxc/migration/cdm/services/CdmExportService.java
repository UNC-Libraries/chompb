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

import com.google.common.collect.Lists;
import edu.unc.lib.boxc.common.util.URIUtil;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.CdmExportOptions;
import edu.unc.lib.boxc.migration.cdm.services.export.ExportState;
import edu.unc.lib.boxc.migration.cdm.services.export.ExportStateService;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static org.slf4j.LoggerFactory.getLogger;
import static edu.unc.lib.boxc.migration.cdm.services.export.ExportState.ProgressState;

/**
 * Service for exporting CDM item records
 * @author bbpennel
 */
public class CdmExportService {
    private static final Logger log = getLogger(CdmExportService.class);

    private CloseableHttpClient httpClient;
    private CdmFieldService cdmFieldService;
    private ExportStateService exportStateService;
    private CdmListIdService listId;
    private MigrationProject project;

    public CdmExportService() {
    }

    /**
     * Export all records from CDM for the provided project
     * @param project
     * @throws IOException
     */
    public void exportAll(CdmExportOptions options) throws IOException {
        // Generate body of export request using the list of fields configure for export
        cdmFieldService.validateFieldsFile(project);
        initializeExportDir(project);
        exportStateService.startOrResumeExport(options.isForce());
        CdmFieldInfo fieldInfo = cdmFieldService.loadFieldsFromProject(project);
        String fieldParams = fieldInfo.getFields().stream()
                .filter(f -> !f.getSkipExport())
                .map(f -> f.getNickName() + "=" + f.getExportAs())
                .collect(Collectors.joining("&"));

        // Add paging to export
        initializeListingService(options);
        List<String> allIds = listId.listAllCdmIds();
        int pageSize = options.getPageSize();
        List<List<String>> chunks = Lists.partition(allIds, pageSize);
        if (exportStateService.inStateOrNotResuming(ProgressState.LISTING_COMPLETED)) {
            exportStateService.transitionToExporting(pageSize);
        }
        if (exportStateService.inStateOrNotResuming(ProgressState.EXPORTING)) {
            int exportPage = 0;
            for (List<String> chunk : chunks) {
                // Name each exported page
                exportPage++;
                if (exportStateService.isResuming()
                        && (exportPage * pageSize - 1) <= exportStateService.getState().getLastExportedIndex()) {
                    continue;
                }
                exportPageOfResults(exportPage, chunk, fieldParams, options);
            }

            project.getProjectProperties().setExportedDate(Instant.now());
            ProjectPropertiesSerialization.write(project);
        }

        exportStateService.exportingCompleted();
    }

    private void exportPageOfResults(int pageNum, List<String> pageIds, String fieldParams, CdmExportOptions options)
            throws IOException {
        String exportFilename = "export_" + pageNum + ".xml";
        String cdmIds = String.join("%2C", pageIds);
        String bodyParams = "CISODB=%2F" + project.getProjectProperties().getCdmCollectionId()
                + "&CISOTYPE=standard&CISOPAGE=1&" + fieldParams + "&CISOPTRLIST=" + cdmIds;
        // Trigger the export
        String exportReqUrl = URIUtil.join(options.getCdmBaseUri(), "cgi-bin/admin/exportxml.exe");
        log.debug("Requesting export from {}", exportReqUrl);
        HttpPost postMethod = new HttpPost(exportReqUrl);
        postMethod.setEntity(new StringEntity(bodyParams, ISO_8859_1));
        try (CloseableHttpResponse resp = httpClient.execute(postMethod)) {
            if (resp.getStatusLine().getStatusCode() != 200) {
                throw new MigrationException("Failed to request export (" + resp.getStatusLine().getStatusCode()
                        + "): " + IOUtils.toString(resp.getEntity().getContent(), ISO_8859_1));
            }
        }
        // Retrieve the export results
        Path exportFilePath = project.getExportPath().resolve(exportFilename);
        downloadExport(options, exportFilePath);

        exportStateService.registerExported(pageIds);
    }

    private void initializeListingService(CdmExportOptions options) {
        listId = new CdmListIdService();
        listId.setHttpClient(httpClient);
        listId.setCdmBaseUri(options.getCdmBaseUri());
        listId.setExportStateService(exportStateService);
        listId.setProject(project);
    }

    private void initializeExportDir(MigrationProject project) throws IOException {
        Files.createDirectories(project.getExportPath());
    }

    private void downloadExport(CdmExportOptions options, Path exportFilePath) throws IOException {
        String uri = URIUtil.join(options.getCdmBaseUri(), "cgi-bin/admin/getfile.exe?")
                + "CISOMODE=1&CISOFILE=/" + project.getProjectProperties().getCdmCollectionId()
                + "/index/description/export.xml";
        log.debug("Downloading export from {}", uri);
        HttpGet getMethod = new HttpGet(uri);
        try (CloseableHttpResponse resp = httpClient.execute(getMethod)) {
            if (resp.getStatusLine().getStatusCode() >= 400) {
                throw new MigrationException("Failed to download export (" + resp.getStatusLine().getStatusCode()
                        + "): " + IOUtils.toString(resp.getEntity().getContent(), ISO_8859_1));
            }
            Files.copy(resp.getEntity().getContent(), exportFilePath, StandardCopyOption.REPLACE_EXISTING);
            log.debug("Downloaded export to file {}", exportFilePath);
        }
    }

    public void setHttpClient(CloseableHttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public void setCdmFieldService(CdmFieldService cdmFieldService) {
        this.cdmFieldService = cdmFieldService;
    }

    public void setExportStateService(ExportStateService exportStateService) {
        this.exportStateService = exportStateService;
    }

    public void setProject(MigrationProject project) {
        this.project = project;
    }
}
