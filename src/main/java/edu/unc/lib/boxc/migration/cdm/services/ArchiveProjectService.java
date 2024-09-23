package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import org.apache.commons.io.FileUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Service for archiving chompb project(s)
 * @author krwong
 */
public class ArchiveProjectService {

    /**
     * Archive a list of projects
     */
    public void archiveProject(Path currentDirectory, List<Path> projectPaths) throws IOException {
        for (Path projectPath : projectPaths) {
            if (Files.notExists(projectPath)) {
                throw new InvalidProjectStateException("Migration project " + projectPath + " does not exist");
            }

            if (Files.isDirectory(projectPath)) {
                FileUtils.moveDirectoryToDirectory(projectPath.toFile(),
                        currentDirectory.resolve("archived").toFile(), true);
            }
        }
    }
}
