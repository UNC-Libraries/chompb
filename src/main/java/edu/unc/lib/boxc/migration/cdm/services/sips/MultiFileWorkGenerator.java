package edu.unc.lib.boxc.migration.cdm.services.sips;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.GroupMappingInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo;
import edu.unc.lib.boxc.migration.cdm.services.CdmIndexService;
import edu.unc.lib.boxc.model.api.ids.PID;
import edu.unc.lib.boxc.model.api.rdf.Cdr;
import edu.unc.lib.boxc.operations.api.order.MemberOrderHelper;
import org.apache.jena.rdf.model.Resource;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * WorkGenerator for works containing multiple files
 *
 * @author bbpennel
 */
public class MultiFileWorkGenerator extends WorkGenerator {
    @Override
    protected void generateWork() throws IOException {
        super.generateWork();
        // Add redirect mapping for compound object, but not group object
        if (!cdmId.startsWith(GroupMappingInfo.GROUPED_WORK_PREFIX)) {
            redirectMappingService.addRow(cdmId, workPid.getId(), null);
        }
    }

    @Override
    protected List<PID> addChildObjects() throws IOException {
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select " + CdmFieldInfo.CDM_ID + "," + queryDateField()
                    + " from " + CdmIndexService.TB_NAME
                    + " where " + CdmIndexService.PARENT_ID_FIELD + " = '" + cdmId + "'"
                    + " order by " + CdmIndexService.CHILD_ORDER_FIELD + " ASC, " + CdmFieldInfo.CDM_ID + " ASC");

            List<PID> childPids = new ArrayList<>();
            while (rs.next()) {
                String fileCdmId = rs.getString(1);
                String cdmCreated = rs.getString(2) + "T00:00:00.000Z";

                SourceFilesInfo.SourceFileMapping sourceMapping = getSourceFileMapping(fileCdmId);
                PID filePid = addFileObject(fileCdmId, cdmCreated, sourceMapping);
                addChildDescription(fileCdmId, filePid);
                postMigrationReportService.addFileRow(fileCdmId, cdmId, workPid.getId(),
                        filePid.getId(), isSingleItem(), sipId);

                childPids.add(filePid);
            }
            return childPids;
        } catch (SQLException e) {
            throw new MigrationException(e);
        }
    }

    // Returns the CDM created date field for CDM projects, or the current date for other types of projects
    private String queryDateField() {
        if (MigrationProject.PROJECT_SOURCE_CDM.equals(project.getProjectProperties().getProjectSource())) {
            return CdmFieldInfo.CDM_CREATED;
        } else {
            return "date('now')";
        }
    }

    @Override
    protected void addFilePermission(String cdmId, Resource resource) {
        addPermission(cdmId, resource);
    }

    protected boolean isSingleItem() {
        return false;
    }
}
