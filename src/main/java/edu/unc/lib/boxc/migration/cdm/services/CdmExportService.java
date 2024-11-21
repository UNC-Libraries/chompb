package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.CdmExportOptions;
import edu.unc.lib.boxc.migration.cdm.services.export.ExportStateService;
import edu.unc.lib.boxc.migration.cdm.services.ChompbConfigService.ChompbConfig;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;

import static edu.unc.lib.boxc.migration.cdm.services.export.ExportState.ProgressState;
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

    public CdmExportService() {
    }

    /**
     * Export all records from CDM for the provided project
     * @param options
     * @throws IOException
     */
    public void exportAll(CdmExportOptions options) throws IOException {
        // Generate body of export request using the list of fields configure for export
        cdmFieldService.validateFieldsFile(project);
        initializeExportDir(project);

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

        exportStateService.exportingCompleted();
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
