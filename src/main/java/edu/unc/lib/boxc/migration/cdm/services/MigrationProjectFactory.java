package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProjectProperties;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.Assert;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

/**
 * Service which provides MigrationProject objects
 *
 * @author bbpennel
 */
public class MigrationProjectFactory {
    private MigrationProjectFactory() {
    }

    public static MigrationProject createCdmMigrationProject(Path path, String name, String collectionId,
                                                             String user, String cdmEnvId, String bxcEnvId)
            throws IOException {
        return createMigrationProject(path, name, collectionId, user, cdmEnvId, bxcEnvId,
                MigrationProject.PROJECT_SOURCE_CDM);
    }

    public static MigrationProject createFilesMigrationProject(Path path, String name, String user, String bxcEnvId)
            throws IOException {
        return createMigrationProject(path, name, null, user, null, bxcEnvId,
                MigrationProject.PROJECT_SOURCE_FILES);
    }

    /**
     * Create a new MigrationProject, initializing a new directory and basic structure
     * @param path base path the project will be written to
     * @param name name of the project
     * @param collectionId id of the cdm collection
     * @param user user performing the migration
     * @param cdmEnvId identifier for cdm environment the data is migrating from
     * @param bxcEnvId identifier for boxc environment the migration is targeting
     * @return
     * @throws IOException
     */
    public static MigrationProject createMigrationProject(Path path, String name,
                                                          String collectionId, String user,
                                                          String cdmEnvId, String bxcEnvId, String projectSource)
            throws IOException {
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
        if (projectSource.equalsIgnoreCase(MigrationProject.PROJECT_SOURCE_CDM)) {
            properties.setCdmCollectionId(collectionId == null ? projectName : collectionId);
        } else {
            properties.setCdmCollectionId(null);
        }
        properties.setCdmEnvironmentId(cdmEnvId);
        properties.setBxcEnvironmentId(bxcEnvId);
        properties.setProjectSource(projectSource);
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
