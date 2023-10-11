package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.DestinationMappingOptions;
import org.slf4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Service for destination matching based on archival collection number
 * @author krwong
 */
public class ArchivalDestinationsService {
    private static final Logger log = getLogger(ArchivalDestinationsService.class);

    private MigrationProject project;
    private CdmIndexService indexService;

    /**
     * Generates a unique list of values in the accepted field name
     * @param options destination mapping options
     * @return A list
     */
    public List<String> generateCollectionNumbersList(DestinationMappingOptions options) {
        List<String> collectionNumbers = new ArrayList<>();

        Connection conn = null;
        try {
            conn = indexService.openDbConnection();
            Statement stmt = conn.createStatement();
            // skip over values from children of compound objects, since they must go to the same destination as their parent work
            ResultSet rs = stmt.executeQuery("select distinct " + options.getFieldName()
                    + " from " + CdmIndexService.TB_NAME
                    + " where " + " ("+ CdmIndexService.ENTRY_TYPE_FIELD + " != '"
                    + CdmIndexService.ENTRY_TYPE_COMPOUND_CHILD + "'" +
                    " OR " + CdmIndexService.ENTRY_TYPE_FIELD + " is null)");
            while (rs.next()) {
                collectionNumbers.add(rs.getString(1));
            }
            return collectionNumbers;
        } catch (SQLException e) {
            throw new MigrationException("Error interacting with export index", e);
        } finally {
            CdmIndexService.closeDbConnection(conn);
        }
    }

    public void setProject(MigrationProject project) {
        this.project = project;
    }

    public void setIndexService(CdmIndexService indexService) {
        this.indexService = indexService;
    }
}
