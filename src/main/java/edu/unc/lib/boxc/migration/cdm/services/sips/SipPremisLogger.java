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
        Path premisPath = destEntry.getDepositDirManager().getPremisPath(pid);
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
