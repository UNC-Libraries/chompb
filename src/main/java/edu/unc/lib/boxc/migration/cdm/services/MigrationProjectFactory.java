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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

import org.apache.commons.lang3.StringUtils;
import org.springframework.util.Assert;

import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProjectProperties;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;

/**
 * Service which provides MigrationProject objects
 *
 * @author bbpennel
 */
public class MigrationProjectFactory {
    private MigrationProjectFactory() {
    }

    /**
     * Create a new MigrationProject, initializing a new directory and basic structure
     * @param path
     * @param name
     * @param collectionId
     * @param user
     * @return
     * @throws IOException
     */
    public static MigrationProject createMigrationProject(Path path, String name,
            String collectionId, String user) throws IOException {
        Assert.notNull(path, "Project path not set");
        Assert.notNull(user, "Username not set");

        String projectName;
        Path projectPath;
        if (StringUtils.isBlank(name)) {
            projectPath = path;
            projectName = projectPath.getFileName().toString();
        } else {
            projectPath = path.resolve(name);
            projectName = name;
        }

        if (Files.notExists(projectPath)) {
            Files.createDirectory(projectPath);
        } else if (!Files.isDirectory(projectPath)) {
            throw new InvalidProjectStateException("Project path " + projectPath
                    + " already exists and is not a directory");
        }

        MigrationProject project = new MigrationProject(projectPath);
        Path propertiesPath = project.getProjectPropertiesPath();
        if (Files.exists(propertiesPath)) {
            throw new InvalidProjectStateException("Cannot create new project at path " + projectPath
                    + ", the directory already contains a migration project");
        }
        // Setup and serialize initial properties
        MigrationProjectProperties properties = new MigrationProjectProperties();
        properties.setCreator(user);
        properties.setCreatedDate(Instant.now());
        properties.setName(projectName);
        properties.setCdmCollectionId(collectionId == null ? projectName : collectionId);
        project.setProjectProperties(properties);
        ProjectPropertiesSerialization.write(propertiesPath, properties);

        // Initialize the descriptions folder
        Files.createDirectories(project.getDescriptionsPath());
        Files.createDirectories(project.getNewCollectionDescriptionsPath());

        return project;
    }

    /**
     * Load an existing MigrationProject from the provided path
     * @param path
     * @return
     * @throws IOException
     */
    public static MigrationProject loadMigrationProject(Path path) throws IOException {
        if (Files.notExists(path)) {
            throw new InvalidProjectStateException("Path " + path + " does not exist");
        }
        MigrationProject project = new MigrationProject(path);
        Path propertiesPath = project.getProjectPropertiesPath();
        if (Files.notExists(propertiesPath)) {
            throw new InvalidProjectStateException("Path " + path + " does not contain an initialized project");
        }
        project.setProjectProperties(ProjectPropertiesSerialization.read(propertiesPath));

        return project;
    }
}
