package edu.unc.lib.boxc.migration.cdm.services;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.CdmExportOptions;
import edu.unc.lib.boxc.migration.cdm.services.export.ExportStateService;
import edu.unc.lib.boxc.migration.cdm.services.ChompbConfigService.ChompbConfig;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Instant;

import static edu.unc.lib.boxc.migration.cdm.services.export.ExportState.ProgressState;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmUtil.STANDARDIZED_CITATION;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmUtil.STANDARDIZED_COLLECTION_NAME;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmUtil.STANDARDIZED_COLLECTION_NUMBER;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmUtil.STANDARDIZED_COLLECTION_URL;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmUtil.STANDARDIZED_CONTAINER;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmUtil.STANDARDIZED_CONTAINER_TYPE;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmUtil.STANDARDIZED_EXTENT;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmUtil.STANDARDIZED_FILENAME;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmUtil.STANDARDIZED_GENRE_FORM;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmUtil.STANDARDIZED_GEOGRAPHIC_NAME;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmUtil.STANDARDIZED_HOOK_ID;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmUtil.STANDARDIZED_LOC_IN_COLLECTION;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmUtil.STANDARDIZED_OBJECT;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmUtil.STANDARDIZED_OBJ_FILENAME;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmUtil.STANDARDIZED_PROCESS_INFO;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmUtil.STANDARDIZED_REF_ID;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmUtil.STANDARDIZED_SCOPE_CONTENT;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmUtil.STANDARDIZED_UNIT_DATE;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmUtil.STANDARDIZED_UNIT_TITLE;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmUtil.TSV_STANDARDIZED_HEADERS;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmUtil.getValue;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Service for exporting CDM item records, either from CDM or EAD to CDM
 * @author bbpennel
 */
public class CdmExportService {
    private static final Logger log = getLogger(CdmExportService.class);

    private CdmFieldService cdmFieldService;
    private ExportStateService exportStateService;
    private CdmFileRetrievalService fileRetrievalService;
    private MigrationProject project;
    private ChompbConfig chompbConfig;

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
            exportFromEadToCdm();
        } else {
           exportFromCdm(options);
           exportStateService.exportingCompleted();
        }
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

    /**
     * This method calls the EAD to CDM API and transforms the JSON to a TSV for indexing
     */
    private void exportFromEadToCdm() {
        var projectName = project.getProjectName();
        var eadId = projectName.split("_")[0];
        var url = chompbConfig.getBxcEnvironments().get(project.getProjectProperties().getBxcEnvironmentId()).getEadToCdmUrl() + eadId;
        var getMethod = new HttpGet(url);
        ObjectMapper mapper = new ObjectMapper();
        var csvPrinterFormat = CSVFormat.TDF.builder()
                .setTrim(true)
                .setHeader(TSV_STANDARDIZED_HEADERS)
                .get();
        var eadToCdmTsvPath = project.getEadToCdmExportPath();

        try (
            var httpClient = HttpClientBuilder.create().disableRedirectHandling().build();
            CloseableHttpResponse resp = httpClient.execute(getMethod);
            var writer = Files.newBufferedWriter(eadToCdmTsvPath);
            CSVPrinter tsvPrinter = new CSVPrinter(writer, csvPrinterFormat);
        ) {
            var body = IOUtils.toString(resp.getEntity().getContent(), StandardCharsets.ISO_8859_1);
            JsonParser parser = mapper.getFactory().createParser(body);
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                throw new MigrationException("Unexpected response from URL " + url
                        + "\nIt must be a JSON object, please check the response.");
            }
            ObjectNode rootNode = mapper.readTree(parser);
            var jsonArray = rootNode.get(eadId);
            if (!jsonArray.isArray()) {
                throw new MigrationException("Unexpected response from URL " + url
                        + "\nJSON value must be a JSON array, please check the response.");
            }
            for (JsonNode jsonNode : jsonArray) {
                ObjectNode entryNode = (ObjectNode) jsonNode;
                tsvPrinter.printRecord(getValue(STANDARDIZED_COLLECTION_NAME, entryNode), getValue(STANDARDIZED_COLLECTION_NUMBER, entryNode),
                    getValue(STANDARDIZED_LOC_IN_COLLECTION, entryNode), getValue(STANDARDIZED_CITATION, entryNode),
                    getValue(STANDARDIZED_FILENAME, entryNode), getValue(STANDARDIZED_OBJ_FILENAME, entryNode), getValue(STANDARDIZED_CONTAINER_TYPE, entryNode),
                    getValue(STANDARDIZED_HOOK_ID, entryNode), getValue(STANDARDIZED_OBJECT, entryNode), getValue(STANDARDIZED_COLLECTION_URL, entryNode),
                    getValue(STANDARDIZED_GENRE_FORM, entryNode), getValue(STANDARDIZED_EXTENT, entryNode), getValue(STANDARDIZED_UNIT_DATE, entryNode),
                    getValue(STANDARDIZED_GEOGRAPHIC_NAME, entryNode), getValue(STANDARDIZED_REF_ID, entryNode), getValue(STANDARDIZED_PROCESS_INFO, entryNode),
                    getValue(STANDARDIZED_SCOPE_CONTENT, entryNode), getValue(STANDARDIZED_UNIT_TITLE, entryNode), getValue(STANDARDIZED_CONTAINER, entryNode));
            }
        } catch (IOException e) {
            log.warn("Failed to retrieve response from EAD to CDM API {}: {}", url, e.getMessage());
            log.debug("Full error", e);
            throw new MigrationException("Unable to export from EAD to CDM", e);
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
}
