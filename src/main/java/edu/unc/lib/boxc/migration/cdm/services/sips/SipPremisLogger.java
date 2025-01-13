package edu.unc.lib.boxc.migration.cdm.services.sips;

import edu.unc.lib.boxc.migration.cdm.model.DestinationSipEntry;
import edu.unc.lib.boxc.migration.cdm.options.SipGenerationOptions;
import edu.unc.lib.boxc.model.api.SoftwareAgentConstants;
import edu.unc.lib.boxc.model.api.ids.PID;
import edu.unc.lib.boxc.model.api.rdf.Premis;
import edu.unc.lib.boxc.model.fcrepo.ids.AgentPids;
import edu.unc.lib.boxc.operations.api.events.PremisLogger;
import edu.unc.lib.boxc.operations.api.events.PremisLoggerFactory;

import java.nio.file.Path;

/**
 * Client for logging premis events during sip generation
 *
 * @author bbpennel
 */
public class SipPremisLogger {
    private PremisLoggerFactory premisLoggerFactory;

    public void addPremisEvent(DestinationSipEntry destEntry, PID pid, SipGenerationOptions options) {
        Path premisPath = destEntry.getDepositDirManager().getPremisPath(pid, true);
        PremisLogger premisLogger = premisLoggerFactory.createPremisLogger(pid, premisPath.toFile());
        premisLogger.buildEvent(Premis.Ingestion)
                .addEventDetail("Object migrated as a part of the CONTENTdm to Box-c 5 migration")
                .addSoftwareAgent(AgentPids.forSoftware(SoftwareAgentConstants.SoftwareAgent.cdmToBxcMigrationUtil))
                .addAuthorizingAgent(AgentPids.forPerson(options.getUsername()))
                .writeAndClose();
    }

    public void setPremisLoggerFactory(PremisLoggerFactory premisLoggerFactory) {
        this.premisLoggerFactory = premisLoggerFactory;
    }
}
