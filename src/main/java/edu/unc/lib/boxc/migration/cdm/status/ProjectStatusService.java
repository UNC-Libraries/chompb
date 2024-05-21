package edu.unc.lib.boxc.migration.cdm.status;

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;

import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProjectProperties;
import edu.unc.lib.boxc.migration.cdm.options.Verbosity;
import edu.unc.lib.boxc.migration.cdm.services.CdmFieldService;
import edu.unc.lib.boxc.migration.cdm.services.DescriptionsService;
import edu.unc.lib.boxc.migration.cdm.services.SipService;
import edu.unc.lib.boxc.migration.cdm.services.StreamingMetadataService;
import org.apache.commons.lang3.StringUtils;

/**
 * Service which displays overall the status of a migration project
 *
 * @author bbpennel
 */
public class ProjectStatusService extends AbstractStatusService {
    private CdmFieldService fieldService;
    private StreamingMetadataService streamingMetadataService;

    public void report() {
        outputLogger.info("Status for project {}", project.getProjectName());

        initializeQueryService();

        MigrationProjectProperties properties = project.getProjectProperties();
        showField("Initialized", properties.getCreatedDate().toString());
        showField("User", properties.getCreator());
        showField("CDM Collection ID", properties.getCdmCollectionId());
        sectionDivider();

        outputLogger.info("CDM Collection Fields");
        fieldService = new CdmFieldService();
        try {
            fieldService.validateFieldsFile(project);
            showField("Mapping File Valid", "Yes");
            CdmFieldInfo fieldInfo = fieldService.loadFieldsFromProject(project);
            showField("Fields", fieldInfo.getFields().size());
            showField("Skipped", fieldInfo.getFields().stream().filter(f -> f.getSkipExport()).count());
        } catch (InvalidProjectStateException e) {
            showField("Mapping File Valid", "No (" + e.getMessage() + ")");
        }
        sectionDivider();

        outputLogger.info("CDM Collection Exports");
        Instant exported = properties.getExportedDate();
        showField("Last Exported", exported == null ? "Not completed" : exported);
        if (exported == null) {
            return;
        }
        sectionDivider();

        outputLogger.info("Index of CDM Objects");
        Instant indexed = properties.getIndexedDate();
        showField("Last Indexed", indexed == null ? "Not completed" : indexed);
        if (indexed == null) {
            return;
        }
        int totalObjects = getQueryService().countIndexedObjects();
        showField("Total Objects", totalObjects);
        Map<String, Integer> typeCounts = getQueryService().countObjectsByType();
        typeCounts.forEach((type, count) -> {
            if (StringUtils.isBlank(type)) {
                showFieldWithPercent("Single Objects", count, totalObjects);
            } else {
                showFieldWithPercent(type, count, totalObjects);
            }
        });
        sectionDivider();

        outputLogger.info("Destination Mappings");
        reportDestinationStats(totalObjects);
        sectionDivider();

        outputLogger.info("Descriptions");
        reportDescriptionStats(totalObjects);
        sectionDivider();

        outputLogger.info("Source File Mappings");
        reportSourceMappings(totalObjects);
        sectionDivider();

        outputLogger.info("Access File Mappings");
        reportAccessMappings(totalObjects);
        sectionDivider();

        outputLogger.info("Grouped Object Mappings");
        reportGroupMappings(totalObjects);
        sectionDivider();

        outputLogger.info("Submission Information Packages");
        Instant sipsGenerated = properties.getSipsGeneratedDate();
        showField("Last Generated", sipsGenerated == null ? "Not completed" : sipsGenerated);
        if (sipsGenerated != null) {
            SipService sipService = new SipService();
            sipService.setProject(project);
            showField("Number of SIPs", sipService.listSips().size());
            showField("SIPs Submitted", properties.getSipsSubmitted().size());
        }
    }

    private void reportDestinationStats(int totalObjects) {
        DestinationsStatusService destStatus = new DestinationsStatusService();
        destStatus.setProject(project);
        destStatus.setQueryService(getQueryService());
        destStatus.reportDestinationStats(totalObjects, Verbosity.QUIET);
    }

    private void reportSourceMappings(int totalObjects) {
        SourceFilesStatusService statusService = new SourceFilesStatusService();
        statusService.setProject(project);
        statusService.setQueryService(getQueryService());
        statusService.setStreamingMetadataService(streamingMetadataService);
        statusService.reportStats(totalObjects, Verbosity.QUIET);
    }

    private void reportAccessMappings(int totalObjects) {
        AccessFilesStatusService statusService = new AccessFilesStatusService();
        statusService.setProject(project);
        statusService.setQueryService(getQueryService());
        statusService.setStreamingMetadataService(streamingMetadataService);
        statusService.reportStats(totalObjects, Verbosity.QUIET);
    }

    private void reportGroupMappings(int totalObjects) {
        GroupMappingStatusService statusService = new GroupMappingStatusService();
        statusService.setProject(project);
        statusService.setQueryService(getQueryService());
        statusService.reportStats(totalObjects, Verbosity.QUIET);
    }

    private void reportDescriptionStats(int totalObjects) {
        DescriptionsService descService = new DescriptionsService();
        descService.setProject(project);
        DescriptionsStatusService descStatus = new DescriptionsStatusService();
        descStatus.setProject(project);
        descStatus.setDescriptionsService(descService);
        descStatus.setQueryService(getQueryService());
        descStatus.reportStats(totalObjects, Verbosity.QUIET);
    }

    public void setStreamingMetadataService(StreamingMetadataService streamingMetadataService) {
        this.streamingMetadataService = streamingMetadataService;
    }
}
