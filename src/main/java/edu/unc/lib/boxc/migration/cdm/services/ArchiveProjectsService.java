package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Service for archiving chompb project(s)
 * @author krwong
 */
public class ArchiveProjectsService {
    private static final Logger log = LoggerFactory.getLogger(ArchiveProjectsService.class);
    public static final String ARCHIVED = ".archived";

    /**
     * Archive a list of projects
     */
    public void archiveProjects(Path currentDirectory, List<String> projectNames) throws IOException {
        for (String projectName : projectNames) {
            Path projectDirectory = currentDirectory.resolve(projectName);
            Path archiveDirectory = currentDirectory.resolve(ARCHIVED);

            if (Files.notExists(projectDirectory)) {
                throw new InvalidProjectStateException("Migration project " + projectName + " does not exist");
            }

            if (Files.isDirectory(projectDirectory)) {
                FileUtils.moveDirectoryToDirectory(projectDirectory.toFile(),
                        archiveDirectory.toFile(), true);
            } else {
                throw new InvalidProjectStateException("Migration project " + projectName + " is not a directory");
            }
        }
    }
}
