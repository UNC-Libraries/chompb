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

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Service for manually updating the project properties
 * @author krwong
 */
public class ProjectPropertiesService {

    private MigrationProject project;

    /**
     * List project properties that can be manually set/unset
     * @throws IOException
     */
    public void getProjectProperties() throws IOException {
        Map<String, String> map = new HashMap<String, String>();
        map.put("hookId", project.getProjectProperties().getHookId());
        map.put("collectionNumber", project.getProjectProperties().getCollectionNumber());
        Set<Map.Entry<String, String>> set = map.entrySet();
        List<Map.Entry<String, String>> configuredProperties = new ArrayList<>(set);
        for (int i = 0; i < configuredProperties.size(); i++) {
            System.out.println(configuredProperties.get(i).getKey() + ": " + configuredProperties.get(i).getValue());
        }
    }

    /**
     * Manually set a project property
     * @param setField
     * @param setValue
     * @throws IOException
     */
    public void setProperties(String setField, String setValue) throws IOException {
        if ((setField.equalsIgnoreCase("hookId") == true) && (setValue != null)) {
            project.getProjectProperties().setHookId(setValue);
            ProjectPropertiesSerialization.write(project);
        } else if ((setField.equalsIgnoreCase("collectionNumber") == true) && (setValue != null)) {
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
            if (unsetField.equalsIgnoreCase("hookId") == true) {
                project.getProjectProperties().setHookId(null);
                ProjectPropertiesSerialization.write(project);
            } else if (unsetField.equalsIgnoreCase("collectionNumber") == true) {
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
