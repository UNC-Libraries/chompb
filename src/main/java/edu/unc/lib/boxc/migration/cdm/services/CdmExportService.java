package edu.unc.lib.boxc.migration.cdm.services;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.CdmExportOptions;
import edu.unc.lib.boxc.migration.cdm.services.export.ExportStateService;
import edu.unc.lib.boxc.migration.cdm.services.ChompbConfigService.ChompbConfig;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Instant;

import static edu.unc.lib.boxc.migration.cdm.services.export.ExportState.ProgressState;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmHeaderConstants.TSV_STANDARDIZED_HEADERS;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Service for exporting CDM item records
 * @author bbpennel
 */
public class CdmExportService {
    private static final Logger log = getLogger(CdmExportService.class);

    private CdmFieldService cdmFieldService;
    private ExportStateService exportStateService;
    private CdmFileRetrievalService fileRetrievalService;
    private MigrationProject project;
    private ChompbConfig chompbConfig;
    private CloseableHttpClient httpClient;

    public CdmExportService() {
    }

    /**
     * Export all records from CDM for the provided project
     * @param options
     * @throws IOException
     */
    public void exportAll(CdmExportOptions options) throws IOException {
        initializeExportDir(project);
        if (options.isEadToCdm()) {
            exportFromEadToCdm(options);
        } else {
           exportFromCdm(options);
        }
        exportStateService.exportingCompleted();
    }

    private void exportFromCdm(CdmExportOptions options) throws IOException {
        // Generate body of export request using the list of fields configure for export
        cdmFieldService.validateFieldsFile(project);

        // Retrieval desc.all file in order to get list of ids
        if (exportStateService.inStateOrNotResuming(ProgressState.STARTING, ProgressState.DOWNLOADING_DESC)) {
            exportStateService.transitionToDownloadingDesc();
            initializeFileRetrievalService(options);
            fileRetrievalService.downloadDescAllFile();
            exportStateService.transitionToDownloadingCpd();
        }

        if (exportStateService.inStateOrNotResuming(ProgressState.DOWNLOADING_CPD)) {
            fileRetrievalService.downloadCpdFiles();
            project.getProjectProperties().setExportedDate(Instant.now());
            ProjectPropertiesSerialization.write(project);
            exportStateService.transitionToDownloadingPdf();
        }

        if (exportStateService.inStateOrNotResuming(ProgressState.DOWNLOADING_PDF)) {
            fileRetrievalService.downloadPdfFiles();
            project.getProjectProperties().setExportedDate(Instant.now());
            ProjectPropertiesSerialization.write(project);
        }
    }

    private void exportFromEadToCdm(CdmExportOptions options) {
        var projectName = project.getProjectName();
        var eadId = projectName.split("_")[0];
        var url = "https://atka.lib.unc.edu/ead-to-cdm/api/" + eadId;
        var getMethod = new HttpGet(url);
        ObjectMapper mapper = new ObjectMapper();

        try (
            CloseableHttpResponse resp = httpClient.execute(getMethod)
        ) {
            var body = IOUtils.toString(resp.getEntity().getContent(), StandardCharsets.ISO_8859_1);
            var csvFormat = CSVFormat.TDF.builder()
                    .setTrim(true)
                    .setSkipHeaderRecord(true)
                    .setHeader(TSV_STANDARDIZED_HEADERS)
                    .get();
            JsonParser parser = mapper.getFactory().createParser(body);
            if (parser.nextToken() != JsonToken.START_ARRAY) {
                throw new MigrationException("Unexpected response from URL " + url
                        + "\nIt must be a JSON array, please check the response.");
            }
            while (parser.nextToken() == JsonToken.START_OBJECT) {

            }
        } catch (IOException e) {
            log.warn("Failed to retrieve response from EAD to CDM API {}: {}", url, e.getMessage());
            log.debug("Full error", e);

        }

    }

    private void initializeFileRetrievalService(CdmExportOptions options) {
        if (fileRetrievalService == null) {
            fileRetrievalService = new CdmFileRetrievalService();
            fileRetrievalService.setSshPassword(options.getCdmPassword());
            fileRetrievalService.setSshUsername(options.getCdmUsername());
            fileRetrievalService.setChompbConfig(chompbConfig);
            fileRetrievalService.setProject(project);
        }
    }

    private void initializeExportDir(MigrationProject project) throws IOException {
        Files.createDirectories(project.getExportPath());
    }

    public void setCdmFieldService(CdmFieldService cdmFieldService) {
        this.cdmFieldService = cdmFieldService;
    }

    public void setExportStateService(ExportStateService exportStateService) {
        this.exportStateService = exportStateService;
    }

    public void setFileRetrievalService(CdmFileRetrievalService fileRetrievalService) {
        this.fileRetrievalService = fileRetrievalService;
    }

    public void setProject(MigrationProject project) {
        this.project = project;
    }

    public void setChompbConfig(ChompbConfig chompbConfig) {
        this.chompbConfig = chompbConfig;
    }

    public void setHttpClient(CloseableHttpClient httpClient) {
        this.httpClient = httpClient;
    }
}
