package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;

/**
 * Service for retrieving streaming metadata
 * @author krwong
 */
public class StreamingMetadataService {
    private static final Logger log = LoggerFactory.getLogger(StreamingMetadataService.class);

    private MigrationProject project;
    private CdmFieldService fieldService;
    private CdmIndexService indexService;

    public static final String STREAMING_FILE_FIELD = "stream";
    public static final String DURACLOUD_SPACE_FIELD = "duracl";
    public static final String PLAYLIST_FILE_EXTENSION = "-playlist.m3u8";
    public static final String DURACLOUD_OPEN = "open-hls";
    public static final String DURACLOUD_CAMPUS = "campus-hls";
    public static final String DURACLOUD_CLOSED = "closed-hls";
    public static final String STREAMING_HOST = "duracloud";

    /**
     * Verify if a record has streaming metadata
     * @param cdmId
     * @return true/false
     */
    public boolean verifyRecordHasStreamingMetadata(String cdmId) throws Exception {
        // check if project has streamingFile field and duracloudSpace field
        fieldService.validateFieldsFile(project);
        CdmFieldInfo fieldInfo = fieldService.loadFieldsFromProject(project);
        List<String> exportFields = fieldInfo.listAllExportFields();
        if (!exportFields.contains(STREAMING_FILE_FIELD) && !exportFields.contains(DURACLOUD_SPACE_FIELD)) {
            return false;
        }

        // check if record has streamingFile field and duracloudSpace field
        Connection conn = null;
        try {
            conn = indexService.openDbConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select " + STREAMING_FILE_FIELD + ", " + DURACLOUD_SPACE_FIELD
                    + " from " + CdmIndexService.TB_NAME
                    + " where " + CdmFieldInfo.CDM_ID + " = " + cdmId);
            while (rs.next()) {
                if (!rs.getString(1).isEmpty() && !rs.getString(2).isEmpty()) {
                    return true;
                }
            }
            return false;
        } catch (SQLException e) {
            throw new MigrationException("Error interacting with export index", e);
        } finally {
            CdmIndexService.closeDbConnection(conn);
        }
    }

    /**
     * Retrieve streaming metadata and remap to correct values
     * @param cdmId
     * @return object with streamingFile, duracloudSpace, and streamingHost fields
     */
    public Object[] getStreamingMetadata(String cdmId) throws Exception {
        String duracloudSpace = null;
        String streamingFile = null;

        // retrieve streaming metadata
        Connection conn = null;
        try {
            conn = indexService.openDbConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select " + STREAMING_FILE_FIELD + ", " + DURACLOUD_SPACE_FIELD
                    + " from " + CdmIndexService.TB_NAME
                    + " where " + CdmFieldInfo.CDM_ID + " = " + cdmId);
            while (rs.next()) {
                if (!rs.getString(1).isEmpty()) {
                    streamingFile = rs.getString(1);
                }
                if (!rs.getString(2).isEmpty()) {
                    duracloudSpace = rs.getString(2);
                }
            }
        } catch (SQLException e) {
            throw new MigrationException("Error interacting with export index", e);
        } finally {
            CdmIndexService.closeDbConnection(conn);
        }

        // transform to playlist file extensions
        streamingFile = streamingFile.split("\\.")[0] + PLAYLIST_FILE_EXTENSION;

        // transform to current duracloud space IDs
        if (duracloudSpace.contains("open")) {
            duracloudSpace = DURACLOUD_OPEN;
        } else if (duracloudSpace.contains("campus")) {
            duracloudSpace = DURACLOUD_CAMPUS;
        } else if (duracloudSpace.contains("closed")) {
            duracloudSpace = DURACLOUD_CLOSED;
        }

        return new Object[] {streamingFile, duracloudSpace, STREAMING_HOST};
    }

    public void setProject(MigrationProject project) {
        this.project = project;
    }

    public void setFieldService(CdmFieldService fieldService) {
        this.fieldService = fieldService;
    }

    public void setIndexService(CdmIndexService indexService) {
        this.indexService = indexService;
    }
}
