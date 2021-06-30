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
    public static final String FIELD_NAMES_FILENAME = "cdm_fields.csv";

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
}
