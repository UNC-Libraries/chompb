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
package edu.unc.lib.boxc.migration.cdm.model;

import java.nio.file.Path;

import com.fasterxml.jackson.annotation.JsonIgnore;

import edu.unc.lib.boxc.migration.cdm.services.SipService;
import edu.unc.lib.boxc.model.api.ids.PID;
import edu.unc.lib.boxc.model.api.ids.PIDConstants;
import edu.unc.lib.boxc.model.fcrepo.ids.PIDs;

/**
 * Object representing a migration SIP
 * @author bbpennel
 */
public class MigrationSip {
    private PID depositPid;
    private String newCollectionLabel;
    private PID newCollectionPid;
    private Path sipPath;
    private PID destinationPid;

    public MigrationSip() {
    }

    public MigrationSip(DestinationSipEntry entry) {
        this.depositPid = entry.getDepositPid();
        this.sipPath = entry.depositDirManager.getDepositDir();
        this.newCollectionPid = entry.getNewCollectionPid();
        this.newCollectionLabel = entry.getNewCollectionId();
        this.destinationPid = entry.getDestinationPid();
    }

    /**
     * @return PID of the deposit contained within this SIP
     */
    @JsonIgnore
    public PID getDepositPid() {
        return depositPid;
    }

    public void setDepositPid(PID depositPid) {
        this.depositPid = depositPid;
    }

    public void setDepositId(String depositId) {
        this.depositPid = PIDs.get(PIDConstants.DEPOSITS_QUALIFIER, depositId);
    }

    public String getDepositId() {
        return depositPid.getId();
    }

    /**
     * @return PID of the new collection being created by this SIP, if one was created
     */
    @JsonIgnore
    public PID getNewCollectionPid() {
        return newCollectionPid;
    }

    public void setNewCollectionPid(PID newCollectionPid) {
        this.newCollectionPid = newCollectionPid;
    }

    public String getNewCollectionId() {
        return newCollectionPid == null ? null : newCollectionPid.getId();
    }

    public void setNewCollectionId(String newCollectionId) {
        if (newCollectionId != null) {
            this.newCollectionPid = PIDs.get(newCollectionId);
        }
    }

    /**
     * @return User provided identifier for the new collection
     */
    public String getNewCollectionLabel() {
        return newCollectionLabel;
    }

    public void setNewCollectionLabel(String newCollectionLabel) {
        this.newCollectionLabel = newCollectionLabel;
    }

    /**
     * @return Path to the directory containing the SIP
     */
    public Path getSipPath() {
        return sipPath;
    }

    public void setSipPath(Path sipPath) {
        this.sipPath = sipPath;
    }

    /**
     * @return Path to the file containing the serialized deposit model for this SIP
     */
    @JsonIgnore
    public Path getModelPath() {
        return sipPath.resolve(SipService.MODEL_EXPORT_NAME);
    }

    /**
     * @return PID of the box-c container this SIP should be submitted to
     */
    @JsonIgnore
    public PID getDestinationPid() {
        return destinationPid;
    }

    public void setDestinationPid(PID destinationPid) {
        this.destinationPid = destinationPid;
    }

    public String getDestinationId() {
        return destinationPid.getId();
    }

    public void setDestinationId(String destinationId) {
        this.destinationPid = PIDs.get(destinationId);
    }
}