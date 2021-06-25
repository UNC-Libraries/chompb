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

    public MigrationProjectProperties() {
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
     * @return user which created this project
     */
    public String getCreator() {
        return creator;
    }

    public void setCreator(String creator) {
        this.creator = creator;
    }
}
