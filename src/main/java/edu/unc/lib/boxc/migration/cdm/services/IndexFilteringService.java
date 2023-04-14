package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.IndexFilteringOptions;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.stream.Collectors;

import static edu.unc.lib.boxc.migration.cdm.services.CdmIndexService.ENTRY_TYPE_COMPOUND_CHILD;
import static edu.unc.lib.boxc.migration.cdm.services.CdmIndexService.ENTRY_TYPE_COMPOUND_OBJECT;
import static edu.unc.lib.boxc.migration.cdm.services.CdmIndexService.ENTRY_TYPE_FIELD;
import static edu.unc.lib.boxc.migration.cdm.services.CdmIndexService.PARENT_ID_FIELD;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Service for filtering the cdm index to a subset of the original items
 * @author bbpennel
 */
public class IndexFilteringService {
    private static final Logger log = getLogger(IndexFilteringService.class);
    protected MigrationProject project;
    private CdmIndexService indexService;

    /**
     * Calculates the number of items that would be left in the index if the provided filter were applied
     * @param options filtering options
     * @return A map with two values, "remainder" => count of items that would remain,
     *      and "total" => count of items before filtering.
     */
    public Map<String, Integer> calculateRemainder(IndexFilteringOptions options) {
        var remainderQuery = buildRemainderQuery(options);
        Connection conn = null;
        try {
            conn = indexService.openDbConnection();
            Statement stmt = conn.createStatement();
            var remainderCount = retrieveCount(stmt, remainderQuery);
            var totalCount = retrieveCount(stmt, buildTotalQuery());

            return Map.of("remainder", remainderCount,
                    "total", totalCount);
        } catch (SQLException e) {
            throw new MigrationException("Error interacting with export index", e);
        } finally {
            CdmIndexService.closeDbConnection(conn);
        }
    }

    private Integer retrieveCount(Statement stmt, String query) throws SQLException {
        ResultSet rs = stmt.executeQuery(query);
        rs.next();
        return rs.getInt(1);
    }

    private String buildRemainderQuery(IndexFilteringOptions options) {
        var queryFilters = buildQueryFilters(options);
        return "select count(*) from ( " +
                    "select dmrecord from " + CdmIndexService.TB_NAME + " where " +
                        " (" + ENTRY_TYPE_FIELD + " != '" + ENTRY_TYPE_COMPOUND_CHILD + "'" +
                        " OR " + ENTRY_TYPE_FIELD + " is null)" +
                        " AND " + queryFilters +
                    " UNION " +
                    " select dmrecord from " + CdmIndexService.TB_NAME + " where " +
                        ENTRY_TYPE_FIELD + " = '" + ENTRY_TYPE_COMPOUND_CHILD + "'" +
                        " AND " + PARENT_ID_FIELD + " in (" +
                            "select dmrecord from " + CdmIndexService.TB_NAME + " where " +
                                ENTRY_TYPE_FIELD + " = '" + ENTRY_TYPE_COMPOUND_OBJECT + "'" +
                                " AND " + queryFilters +
                        " ) " +
                " ) ";
    }

    private String buildTotalQuery() {
        return "select count(*) from cdm_records";
    }

    private String buildQueryFilters(IndexFilteringOptions options) {
        var includeFilter = !CollectionUtils.isEmpty(options.getIncludeValues());
        var filterValues = includeFilter ? options.getIncludeValues() : options.getExcludeValues();
        var fieldName = options.getFieldName();
        var query = filterValues.stream()
                           .map(v -> fieldName + " = '" + v.replace("'", "\\'") + "'")
                           .collect(Collectors.joining(" OR "));
        return (includeFilter ? "" : " NOT ") + '(' + query + ')';
    }

    /**
     * Filter the index to a subset of the original items based on the provided filtering options
     * @param options filtering options
     */
    public void filterIndex(IndexFilteringOptions options) {

    }

    public void setProject(MigrationProject project) {
        this.project = project;
    }

    public void setIndexService(CdmIndexService indexService) {
        this.indexService = indexService;
    }
}
