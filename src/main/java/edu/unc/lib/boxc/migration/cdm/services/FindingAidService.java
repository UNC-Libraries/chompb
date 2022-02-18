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

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;

import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Service for recording a collection's finding aid
 * @author krwong
 */
public class FindingAidService {
    public static final String CONTRI_FIELD = "contri";
    public static final String DESCRI_FIELD = "descri";
    public static final String HOOK_ID = "hookid";
    public static final String COLLECTION_NUMBER = "collection number";

    private MigrationProject project;
    private CdmFieldService fieldService;

    public void recordFindingAid() throws Exception {
        fieldService.validateFieldsFile(project);
        CdmFieldInfo fieldInfo = fieldService.loadFieldsFromProject(project);
        Map<String, String> fields = fieldInfo.getFields().stream()
                .filter(f -> !f.getSkipExport())
                .collect(Collectors.toMap(CdmFieldInfo.CdmFieldEntry::getNickName,
                        CdmFieldInfo.CdmFieldEntry::getDescription));

        for (Map.Entry<String, String> entry : fields.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key.equalsIgnoreCase(CONTRI_FIELD) && value.equalsIgnoreCase(HOOK_ID)) {
                project.getProjectProperties().setHookId(key);
                ProjectPropertiesSerialization.write(project);
                outputLogger.info("HookId was set. Use 'config list' to view the project property.");
            } else if (key.equalsIgnoreCase(DESCRI_FIELD) && value.equalsIgnoreCase(COLLECTION_NUMBER)) {
                project.getProjectProperties().setCollectionNumber(key);
                ProjectPropertiesSerialization.write(project);
                outputLogger.info("CollectionNumber was set. Use 'config list' to view the project property.");
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
