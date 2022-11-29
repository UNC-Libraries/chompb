package edu.unc.lib.boxc.migration.cdm.services;

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;

import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Service for recording a collection's finding aid
 * @author krwong
 */
public class FindingAidService {
    public static final String CONTRI_FIELD = "contri";
    public static final String DESCRI_FIELD = "descri";
    public static final String HOOK_ID_FIELD_DESC = "hookid";
    public static final String COLLECTION_NUMBER_FIELD_DESC = "collection number";

    private MigrationProject project;
    private CdmFieldService fieldService;

    /**
     * Record a project's contri/descri fields if they're named HookID/Collection Number
     * @throws IOException
     */
    public void recordFindingAid() throws IOException {
        fieldService.validateFieldsFile(project);
        CdmFieldInfo fieldInfo = fieldService.loadFieldsFromProject(project);
        Map<String, String> fields = fieldInfo.getFields().stream()
                .filter(f -> !f.getSkipExport())
                .collect(Collectors.toMap(CdmFieldInfo.CdmFieldEntry::getNickName,
                        CdmFieldInfo.CdmFieldEntry::getDescription));

        for (Map.Entry<String, String> entry : fields.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key.equalsIgnoreCase(CONTRI_FIELD) && value.equalsIgnoreCase(HOOK_ID_FIELD_DESC)) {
                project.getProjectProperties().setHookId(key);
                ProjectPropertiesSerialization.write(project);
                outputLogger.info(ProjectPropertiesService.HOOK_ID +
                        " was set. Use 'config list' to view the project property.");
            } else if (key.equalsIgnoreCase(DESCRI_FIELD) && value.equalsIgnoreCase(COLLECTION_NUMBER_FIELD_DESC)) {
                project.getProjectProperties().setCollectionNumber(key);
                ProjectPropertiesSerialization.write(project);
                outputLogger.info(ProjectPropertiesService.COLLECTION_NUMBER +
                        " was set. Use 'config list' to view the project property.");
            }
        }
    }

    public void setProject(MigrationProject project) {
        this.project = project;
    }

    public void setCdmFieldService(CdmFieldService fieldService) {
        this.fieldService = fieldService;
    }
}
