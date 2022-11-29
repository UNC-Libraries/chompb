package edu.unc.lib.boxc.migration.cdm.services.sips;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.GroupMappingInfo;
import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo;
import edu.unc.lib.boxc.migration.cdm.services.CdmIndexService;
import edu.unc.lib.boxc.model.api.ids.PID;

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
            ResultSet rs = stmt.executeQuery("select " + CdmFieldInfo.CDM_ID + "," + CdmFieldInfo.CDM_CREATED
                    + " from " + CdmIndexService.TB_NAME
                    + " where " + CdmIndexService.PARENT_ID_FIELD + " = '" + cdmId + "'");

            List<PID> childPids = new ArrayList<>();
            while (rs.next()) {
                String fileCdmId = rs.getString(1);
                String cdmCreated = rs.getString(2) + "T00:00:00.000Z";

                SourceFilesInfo.SourceFileMapping sourceMapping = getSourceFileMapping(fileCdmId);
                PID filePid = addFileObject(fileCdmId, cdmCreated, sourceMapping);
                addChildDescription(fileCdmId, filePid);
                postMigrationReportService.addFileRow(fileCdmId, cdmId, workPid.getId(),
                        filePid.getId(), isSingleItem());

                childPids.add(filePid);
            }
            return childPids;
        } catch (SQLException e) {
            throw new MigrationException(e);
        }
    }

    protected boolean isSingleItem() {
        return false;
    }
}
