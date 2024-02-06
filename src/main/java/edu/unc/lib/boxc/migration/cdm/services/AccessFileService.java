package edu.unc.lib.boxc.migration.cdm.services;

import java.io.IOException;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;

/**
 * Service for interacting with access copy files
 * @author bbpennel
 */
public class AccessFileService extends SourceFileService {

    public AccessFileService() {
    }

    @Override
    protected void setUpdatedDate(Instant timestamp) throws IOException {
        project.getProjectProperties().setAccessFilesUpdatedDate(timestamp);
        ProjectPropertiesSerialization.write(project);
    }

    @Override
    public Path getMappingPath() {
        return project.getAccessFilesMappingPath();
    }

    /**
     * @param path
     * @return Mimetype of the provided file
     * @throws IOException
     */
    public String getMimetype(Path path) throws IOException {
        String mimetype = URLConnection.guessContentTypeFromName(path.getFileName().toString());
        if (mimetype == null || "application/octet-stream".equals(mimetype)) {
            mimetype = Files.probeContentType(path);
        }
        if (mimetype == null) {
            mimetype = "application/octet-stream";
        }
        return mimetype;
    }
}
