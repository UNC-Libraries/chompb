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
package edu.unc.lib.boxc.migration.cdm.model;

import java.nio.file.Path;

/**
 * A CDM to Box-c migration project state object
 *
 * @author bbpennel
 */
public class MigrationProject {
    public static final String PROJECT_PROPERTIES_FILENAME = "project.json";
    public static final String DESCRIPTION_DIRNAME = "descriptions";
    public static final String EXPANDED_DESCS_DIRNAME = ".expanded_descs";
    public static final String EXPORT_DIRNAME = "exports";
    public static final String FIELD_NAMES_FILENAME = "cdm_fields.csv";
    public static final String INDEX_FILENAME = "cdm_index.db";
    public static final String DESTINATIONS_FILENAME = "destinations.csv";
    public static final String SOURCE_MAPPING_FILENAME = "source_files.csv";
    public static final String ACCESS_MAPPING_FILENAME = "access_files.csv";
    public static final String SIPS_DIRNAME = "sips";

    private Path projectPath;
    private MigrationProjectProperties properties;

    public MigrationProject(Path projectPath) {
        this.projectPath = projectPath;
    }

    /**
     * @return Path to the directory containing this project
     */
    public Path getProjectPath() {
        return projectPath;
    }

    /**
     * @return Path to file where CDM field name mappings are configured
     */
    public Path getFieldsPath() {
        return projectPath.resolve(FIELD_NAMES_FILENAME);
    }

    /**
     * @return Path of the project properties file
     */
    public Path getProjectPropertiesPath() {
        return projectPath.resolve(PROJECT_PROPERTIES_FILENAME);
    }

    /**
     * @return Path to directory where CDM Record exports should be stored
     */
    public Path getExportPath() {
        return projectPath.resolve(EXPORT_DIRNAME);
    }

    /**
     * @return Properties describing this project
     */
    public MigrationProjectProperties getProjectProperties() {
        return properties;
    }

    public void setProjectProperties(MigrationProjectProperties properties) {
        this.properties = properties;
    }

    /**
     * @return Path of the MODS descriptions directory
     */
    public Path getDescriptionsPath() {
        return projectPath.resolve(DESCRIPTION_DIRNAME);
    }

    /**
     * @return Path where the expanded descriptions files should be stored
     */
    public Path getExpandedDescriptionsPath() {
        return projectPath.resolve(EXPANDED_DESCS_DIRNAME);
    }

    /**
     * @return Path of the index containing exported CDM data
     */
    public Path getIndexPath() {
        return projectPath.resolve(INDEX_FILENAME);
    }

    /**
     * @return Path of the destination mappings file
     */
    public Path getDestinationMappingsPath() {
        return projectPath.resolve(DESTINATIONS_FILENAME);
    }

    /**
     * @return Path of the source files mapping file
     */
    public Path getSourceFilesMappingPath() {
        return projectPath.resolve(SOURCE_MAPPING_FILENAME);
    }

    /**
     * @return Path of the access files mapping file
     */
    public Path getAccessFilesMappingPath() {
        return projectPath.resolve(ACCESS_MAPPING_FILENAME);
    }

    /**
     * @return Path of the SIPS directory
     */
    public Path getSipsPath() {
        return projectPath.resolve(SIPS_DIRNAME);
    }

    /**
     * @return Name of the project
     */
    public String getProjectName() {
        return properties.getName();
    }
}
