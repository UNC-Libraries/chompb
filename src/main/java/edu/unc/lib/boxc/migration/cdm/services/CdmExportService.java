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

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;

import edu.unc.lib.boxc.common.util.URIUtil;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;

/**
 * Service for exporting CDM item records
 * @author bbpennel
 */
public class CdmExportService {
    private static final Logger log = getLogger(CdmExportService.class);

    private CloseableHttpClient httpClient;
    private String cdmBaseUri;
    private CdmFieldService cdmFieldService;

    public CdmExportService() {
    }

    /**
     * Export all records from CDM for the provided project
     * @param project
     * @throws IOException
     */
    public void exportAll(MigrationProject project) throws IOException {
        // Generate body of export request using the list of fields configure for export
        cdmFieldService.validateFieldsFile(project);
        CdmFieldInfo fieldInfo = cdmFieldService.loadFieldsFromProject(project);
        String fieldParams = fieldInfo.getFields().stream()
                .filter(f -> !f.getSkipExport())
                .map(f -> f.getNickName() + "=" + f.getExportAs())
                .collect(Collectors.joining("&"));
        String bodyParams = "CISODB=%2F" + project.getProjectProperties().getCdmCollectionId()
                + "&CISOTYPE=standard&CISOPAGE=1&" + fieldParams;

        // Trigger the export
        String exportReqUrl = URIUtil.join(cdmBaseUri, "cgi-bin/admin/exportxml.exe");
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
        Path exportFilePath = project.getExportPath().resolve("export_all.xml");
        downloadExport(project, exportFilePath);

        project.getProjectProperties().setExportedDate(Instant.now());
        ProjectPropertiesSerialization.write(project);
    }

    private void initializeExportDir(MigrationProject project) throws IOException {
        Files.createDirectories(project.getExportPath());
    }

    private void downloadExport(MigrationProject project, Path exportFilePath) throws IOException {
        String uri = URIUtil.join(cdmBaseUri, "cgi-bin/admin/getfile.exe?")
                + "CISOMODE=1&CISOFILE=/" + project.getProjectProperties().getCdmCollectionId()
                + "/index/description/export.xml";
        log.debug("Downloading export from {}", uri);
        HttpGet getMethod = new HttpGet(uri);
        try (CloseableHttpResponse resp = httpClient.execute(getMethod)) {
            if (resp.getStatusLine().getStatusCode() >= 400) {
                throw new MigrationException("Failed to download export (" + resp.getStatusLine().getStatusCode()
                        + "): " + IOUtils.toString(resp.getEntity().getContent(), ISO_8859_1));
            }
            initializeExportDir(project);
            Files.copy(resp.getEntity().getContent(), exportFilePath, StandardCopyOption.REPLACE_EXISTING);
            log.debug("Downloaded export to file {}", exportFilePath);
        }
    }

    public void setHttpClient(CloseableHttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public void setCdmBaseUri(String cdmBaseUri) {
        this.cdmBaseUri = cdmBaseUri;
    }

    public void setCdmFieldService(CdmFieldService cdmFieldService) {
        this.cdmFieldService = cdmFieldService;
    }
}
