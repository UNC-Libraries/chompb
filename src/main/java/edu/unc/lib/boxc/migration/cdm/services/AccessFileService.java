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
    protected Path getMappingPath() {
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
