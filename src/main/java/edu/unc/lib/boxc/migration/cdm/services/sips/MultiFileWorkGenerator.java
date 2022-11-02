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
                String cdmId = rs.getString(1);
                String cdmCreated = rs.getString(2) + "T00:00:00.000Z";

                SourceFilesInfo.SourceFileMapping sourceMapping = getSourceFileMapping(cdmId);
                PID filePid = addFileObject(cdmId, cdmCreated, sourceMapping);
                addChildDescription(cdmId, filePid);

                childPids.add(filePid);
            }
            return childPids;
        } catch (SQLException e) {
            throw new MigrationException(e);
        }
    }
}
