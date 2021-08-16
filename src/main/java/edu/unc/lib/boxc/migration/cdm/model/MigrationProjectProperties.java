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

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

/**
 * Properties representing the state of a migration project
 *
 * @author bbpennel
 */
public class MigrationProjectProperties {
    private String name;
    private String cdmCollectionId;
    private Instant createdDate;
    private String creator;
    private Instant exportedDate;
    private Instant indexedDate;
    private Instant destinationsGeneratedDate;
    private Instant sourceFilesUpdatedDate;
    private Instant accessFilesUpdatedDate;
    private Instant descriptionsExpandedDate;
    private Instant sipsGeneratedDate;
    private Set<String> sipsSubmitted;

    public MigrationProjectProperties() {
        sipsSubmitted = new HashSet<>();
    }

    /**
     * @return Name of the project
     */
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return Identifier of the CDM collection being migrated
     */
    public String getCdmCollectionId() {
        return cdmCollectionId;
    }

    public void setCdmCollectionId(String cdmCollectionId) {
        this.cdmCollectionId = cdmCollectionId;
    }

    /**
     * @return timestamp the migration project was created
     */
    public Instant getCreatedDate() {
        return createdDate;
    }

    public void setCreatedDate(Instant createdDate) {
        this.createdDate = createdDate;
    }

    /**
     * @return timestamp of the last full successful CDM record export
     */
    public Instant getExportedDate() {
        return exportedDate;
    }

    public void setExportedDate(Instant exportedDate) {
        this.exportedDate = exportedDate;
    }

    /**
     * @return timestamp of the last full successful CDM record indexing
     */
    public Instant getIndexedDate() {
        return indexedDate;
    }

    public void setIndexedDate(Instant indexedDate) {
        this.indexedDate = indexedDate;
    }

    /**
     * @return user which created this project
     */
    public String getCreator() {
        return creator;
    }

    public void setCreator(String creator) {
        this.creator = creator;
    }

    /**
     * @return timestamp the destination mapping was last generated
     */
    public Instant getDestinationsGeneratedDate() {
        return destinationsGeneratedDate;
    }

    public void setDestinationsGeneratedDate(Instant destinationsGeneratedDate) {
        this.destinationsGeneratedDate = destinationsGeneratedDate;
    }

    /**
     * @return timestamp the source files mapping was last updated
     */
    public Instant getSourceFilesUpdatedDate() {
        return sourceFilesUpdatedDate;
    }

    public void setSourceFilesUpdatedDate(Instant sourceFilesGeneratedDate) {
        this.sourceFilesUpdatedDate = sourceFilesGeneratedDate;
    }

    /**
     * @return timestamp the access files mapping was last updated
     */
    public Instant getAccessFilesUpdatedDate() {
        return accessFilesUpdatedDate;
    }

    public void setAccessFilesUpdatedDate(Instant accessFilesUpdatedDate) {
        this.accessFilesUpdatedDate = accessFilesUpdatedDate;
    }

    /**
     * @return timestamp that description files were expanded
     */
    public Instant getDescriptionsExpandedDate() {
        return descriptionsExpandedDate;
    }

    public void setDescriptionsExpandedDate(Instant descriptionsExpandedDate) {
        this.descriptionsExpandedDate = descriptionsExpandedDate;
    }

    /**
     * @return timestamp SIPS were last generated
     */
    public Instant getSipsGeneratedDate() {
        return sipsGeneratedDate;
    }

    public void setSipsGeneratedDate(Instant sipsGeneratedDate) {
        this.sipsGeneratedDate = sipsGeneratedDate;
    }

    /**
     * @return IDs of SIPs which have been submitted
     */
    public Set<String> getSipsSubmitted() {
        return sipsSubmitted;
    }

    public void setSipsSubmitted(Set<String> sipsSubmitted) {
        this.sipsSubmitted = sipsSubmitted;
    }
}
