package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Service for manually updating the project properties
 * @author krwong
 */
public class ProjectPropertiesService {
    private MigrationProject project;

    public static final String HOOK_ID = "hookId";
    public static final String COLLECTION_NUMBER = "collectionNumber";

    /**
     * List project properties that can be manually set/unset
     * @throws IOException
     */
    public Map<String, String> getProjectProperties() throws IOException {
        Map<String, String> projectProperties = new HashMap<>();
        projectProperties.put(HOOK_ID, project.getProjectProperties().getHookId());
        projectProperties.put(COLLECTION_NUMBER, project.getProjectProperties().getCollectionNumber());
        return projectProperties;
    }

    /**
     * Manually set a project property
     * @param setField
     * @param setValue
     * @throws IOException
     */
    public void setProperty(String setField, String setValue) throws IOException {
        if ((HOOK_ID.equalsIgnoreCase(setField)) && (setValue != null)) {
            project.getProjectProperties().setHookId(setValue);
            ProjectPropertiesSerialization.write(project);
        } else if ((COLLECTION_NUMBER.equalsIgnoreCase(setField)) && (setValue != null)) {
            project.getProjectProperties().setCollectionNumber(setValue);
            ProjectPropertiesSerialization.write(project);
        } else {
            throw new MigrationException("Invalid field/value input");
        }
    }

    /**
     * Manually unset/clear project properties
     * @param unsetFields
     * @throws IOException
     */
    public void unsetProperties(List<String> unsetFields) throws IOException {
        for (String unsetField : unsetFields) {
            if (unsetField.equalsIgnoreCase(HOOK_ID)) {
                project.getProjectProperties().setHookId(null);
                ProjectPropertiesSerialization.write(project);
            } else if (unsetField.equalsIgnoreCase(COLLECTION_NUMBER)) {
                project.getProjectProperties().setCollectionNumber(null);
                ProjectPropertiesSerialization.write(project);
            } else {
                throw new MigrationException("Invalid project property(ies)");
            }
        }
    }

    public void setProject(MigrationProject project) {
        this.project = project;
    }
}
