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

import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo;
import edu.unc.lib.boxc.migration.cdm.options.SipGenerationOptions;
import edu.unc.lib.boxc.migration.cdm.services.AccessFileService;
import edu.unc.lib.boxc.migration.cdm.services.CdmIndexService;
import edu.unc.lib.boxc.migration.cdm.services.DescriptionsService;
import edu.unc.lib.boxc.migration.cdm.services.PostMigrationReportService;
import edu.unc.lib.boxc.migration.cdm.services.RedirectMappingService;
import edu.unc.lib.boxc.model.api.ids.PIDMinter;

import java.sql.Connection;

/**
 * Factory which produces WorkGenerator objects
 *
 * @author bbpennel
 */
public class WorkGeneratorFactory {
    private SourceFilesInfo sourceFilesInfo;
    private SourceFilesInfo accessFilesInfo;
    private AccessFileService accessFileService;
    private Connection conn;
    private SipGenerationOptions options;
    private CdmToDestMapper cdmToDestMapper;
    private SipPremisLogger sipPremisLogger;
    private DescriptionsService descriptionsService;
    private RedirectMappingService redirectMappingService;
    private PostMigrationReportService postMigrationReportService;
    private PIDMinter pidMinter;

    public WorkGenerator create(String cdmId, String cdmCreated, String entryType) {
        WorkGenerator gen;
        if (CdmIndexService.ENTRY_TYPE_COMPOUND_OBJECT.equals(entryType)
                || CdmIndexService.ENTRY_TYPE_GROUPED_WORK.equals(entryType)) {
            gen = new MultiFileWorkGenerator();
        } else {
            gen = new WorkGenerator();
        }
        gen.accessFilesInfo = accessFilesInfo;
        gen.accessFileService = accessFileService;
        gen.sourceFilesInfo = sourceFilesInfo;
        gen.descriptionsService = descriptionsService;
        gen.conn = conn;
        gen.options = options;
        gen.destEntry = cdmToDestMapper.getDestinationEntry(cdmId);
        gen.cdmId = cdmId;
        gen.cdmCreated = cdmCreated;
        gen.sipPremisLogger = sipPremisLogger;
        gen.pidMinter = pidMinter;
        gen.redirectMappingService = redirectMappingService;
        gen.postMigrationReportService = postMigrationReportService;
        return gen;
    }

    public void setSourceFilesInfo(SourceFilesInfo sourceFilesInfo) {
        this.sourceFilesInfo = sourceFilesInfo;
    }

    public void setAccessFilesInfo(SourceFilesInfo accessFilesInfo) {
        this.accessFilesInfo = accessFilesInfo;
    }

    public void setConn(Connection conn) {
        this.conn = conn;
    }

    public void setOptions(SipGenerationOptions options) {
        this.options = options;
    }

    public void setCdmToDestMapper(CdmToDestMapper cdmToDestMapper) {
        this.cdmToDestMapper = cdmToDestMapper;
    }

    public void setSipPremisLogger(SipPremisLogger sipPremisLogger) {
        this.sipPremisLogger = sipPremisLogger;
    }

    public void setAccessFileService(AccessFileService accessFileService) {
        this.accessFileService = accessFileService;
    }

    public void setDescriptionsService(DescriptionsService descriptionsService) {
        this.descriptionsService = descriptionsService;
    }

    public void setPidMinter(PIDMinter pidMinter) {
        this.pidMinter = pidMinter;
    }

    public void setRedirectMappingService(RedirectMappingService redirectMappingService) {
        this.redirectMappingService = redirectMappingService;
    }

    public void setPostMigrationReportService(PostMigrationReportService postMigrationReportService) {
        this.postMigrationReportService = postMigrationReportService;
    }
}
