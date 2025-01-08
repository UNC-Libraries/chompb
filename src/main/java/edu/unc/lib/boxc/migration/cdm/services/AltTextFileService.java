package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Service for interacting with alt-text files
 * @author krwong
 */
public class AltTextFileService extends SourceFileService {
    private static final Logger log = LoggerFactory.getLogger(AltTextFileService.class);

    private CdmIndexService indexService;
    private List<String> ids;

    public AltTextFileService() {
    }

    /**
     * Generate the alt-text file mapping template
     * @throws Exception
     */
    public void generateAltTextMapping() throws Exception {
        assertProjectStateValid();
        Path mappingPath = getMappingPath();

        try (var csvPrinter = openMappingsPrinter(mappingPath)) {
            for (var id : getIds()) {
                csvPrinter.printRecord(id, null, null, null);
            }
        }

        setUpdatedDate(Instant.now());
    }

    @Override
    protected void setUpdatedDate(Instant timestamp) throws IOException {
        project.getProjectProperties().setAltTextFilesUpdatedDate(timestamp);
        ProjectPropertiesSerialization.write(project);
    }

    @Override
    public Path getMappingPath() {
        return project.getAltTextFilesMappingPath();
    }

    /**
     * @param path
     * @return Mimetype of the provided file
     * @throws IOException
     */
    public String getMimetype(Path path) throws IOException {
        String mimetype = URLConnection.guessContentTypeFromName(path.getFileName().toString());
        if (mimetype == null || "text/plain".equals(mimetype)) {
            mimetype = Files.probeContentType(path);
        }
        if (mimetype == null) {
            mimetype = "text/plain";
        }
        return mimetype;
    }

    private List<String> getIds() {
        if (ids == null) {
            ids = new ArrayList<>();
            String query = "select " + CdmFieldInfo.CDM_ID + " from " + CdmIndexService.TB_NAME;

            getIndexService();
            try (Connection conn = indexService.openDbConnection()) {
                var stmt = conn.createStatement();
                var rs = stmt.executeQuery(query);
                while (rs.next()) {
                    if (!rs.getString(1).isEmpty()) {
                        ids.add(rs.getString(1));
                    }
                }
                return ids;
            } catch (SQLException e) {
                throw new MigrationException("Error interacting with export index", e);
            }
        }
        return ids;
    }

    private CdmIndexService getIndexService() {
        if (indexService == null) {
            indexService = new CdmIndexService();
            indexService.setProject(project);
        }
        return indexService;
    }
}