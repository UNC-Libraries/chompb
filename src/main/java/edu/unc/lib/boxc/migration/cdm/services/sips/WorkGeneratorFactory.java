package edu.unc.lib.boxc.migration.cdm.services.sips;

import edu.unc.lib.boxc.migration.cdm.model.AltTextInfo;
import edu.unc.lib.boxc.migration.cdm.model.PermissionsInfo;
import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo;
import edu.unc.lib.boxc.migration.cdm.options.SipGenerationOptions;
import edu.unc.lib.boxc.migration.cdm.services.AccessFileService;
import edu.unc.lib.boxc.migration.cdm.services.AggregateFileMappingService;
import edu.unc.lib.boxc.migration.cdm.services.AltTextService;
import edu.unc.lib.boxc.migration.cdm.services.BoxctronFileService;
import edu.unc.lib.boxc.migration.cdm.services.CdmIndexService;
import edu.unc.lib.boxc.migration.cdm.services.DescriptionsService;
import edu.unc.lib.boxc.migration.cdm.services.PostMigrationReportService;
import edu.unc.lib.boxc.migration.cdm.services.RedirectMappingService;
import edu.unc.lib.boxc.migration.cdm.services.StreamingMetadataService;
import edu.unc.lib.boxc.model.api.ids.PIDMinter;

import java.io.IOException;
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
    private AltTextInfo altTextInfo;
    private AltTextService altTextService;
    private BoxctronFileService boxctronFileService;
    private Connection conn;
    private SipGenerationOptions options;
    private CdmToDestMapper cdmToDestMapper;
    private SipPremisLogger sipPremisLogger;
    private DescriptionsService descriptionsService;
    private RedirectMappingService redirectMappingService;
    private PostMigrationReportService postMigrationReportService;
    private AggregateFileMappingService aggregateTopMappingService;
    private AggregateFileMappingService aggregateBottomMappingService;
    private PIDMinter pidMinter;
    private PermissionsInfo permissionsInfo;
    private StreamingMetadataService streamingMetadataService;

    public WorkGenerator create(String cdmId, String cdmCreated, String entryType) throws IOException {
        WorkGenerator gen;
        if (CdmIndexService.ENTRY_TYPE_COMPOUND_OBJECT.equals(entryType) || CdmIndexService.ENTRY_TYPE_GROUPED_WORK.equals(entryType)) {
            gen = new OrderedWorkGenerator();
            ((OrderedWorkGenerator) gen).setAggregateBottomMappings(aggregateBottomMappingService.loadMappings());
            ((OrderedWorkGenerator) gen).setAggregateTopMappings(aggregateTopMappingService.loadMappings());
        } else {
            gen = new WorkGenerator();
        }
        gen.accessFilesInfo = accessFilesInfo;
        gen.accessFileService = accessFileService;
        gen.sourceFilesInfo = sourceFilesInfo;
        gen.altTextInfo = altTextInfo;
        gen.altTextService = altTextService;
        gen.boxctronFileService = boxctronFileService;
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
        gen.permissionsInfo = permissionsInfo;
        gen.streamingMetadataService = streamingMetadataService;
        return gen;
    }

    public void setSourceFilesInfo(SourceFilesInfo sourceFilesInfo) {
        this.sourceFilesInfo = sourceFilesInfo;
    }

    public void setAccessFilesInfo(SourceFilesInfo accessFilesInfo) {
        this.accessFilesInfo = accessFilesInfo;
    }

    public void setAltTextInfo(AltTextInfo altTextInfo) {
        this.altTextInfo = altTextInfo;
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

    public void setAltTextService(AltTextService altTextService) {
        this.altTextService = altTextService;
    }

    public void setBoxctronFileService(BoxctronFileService boxctronFileService) {
        this.boxctronFileService = boxctronFileService;
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

    public void setAggregateTopMappingService(AggregateFileMappingService aggregateTopMappingService) {
        this.aggregateTopMappingService = aggregateTopMappingService;
    }

    public void setAggregateBottomMappingService(AggregateFileMappingService aggregateBottomMappingService) {
        this.aggregateBottomMappingService = aggregateBottomMappingService;
    }

    public void setPermissionsInfo(PermissionsInfo permissionsInfo) {
        this.permissionsInfo = permissionsInfo;
    }

    public void setStreamingMetadataService(StreamingMetadataService streamingMetadataService) {
        this.streamingMetadataService = streamingMetadataService;
    }
}
