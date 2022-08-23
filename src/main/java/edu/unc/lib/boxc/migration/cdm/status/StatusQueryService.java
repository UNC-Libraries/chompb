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
package edu.unc.lib.boxc.migration.cdm.status;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.CdmIndexService;

import static edu.unc.lib.boxc.migration.cdm.services.CdmIndexService.ENTRY_TYPE_COMPOUND_CHILD;
import static edu.unc.lib.boxc.migration.cdm.services.CdmIndexService.ENTRY_TYPE_FIELD;

/**
 * Helper service for performing queries against the index for status services
 *
 * @author bbpennel
 */
public class StatusQueryService {
    private CdmIndexService indexService;
    protected MigrationProject project;

    public StatusQueryService(MigrationProject project) {
        this.project = project;
        this.indexService = new CdmIndexService();
        this.indexService.setProject(project);
    }

    // count all objects, including grouped/compound objects
    private Integer indexedObjectsCountCache;
    protected int countIndexedObjects() {
        if (indexedObjectsCountCache != null) {
            return indexedObjectsCountCache;
        }
        CdmIndexService indexService = new CdmIndexService();
        indexService.setProject(project);
        try (Connection conn = indexService.openDbConnection()) {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select count(*) from " + CdmIndexService.TB_NAME);
            indexedObjectsCountCache = rs.getInt(1);
            return indexedObjectsCountCache;
        } catch (SQLException e) {
            throw new MigrationException("Failed to determine number of objects", e);
        }
    }

    // Count only file objects, exclude grouped/compound objects
    private Integer indexedFileObjectsCountCache;
    protected int countIndexedFileObjects() {
        if (indexedFileObjectsCountCache != null) {
            return indexedFileObjectsCountCache;
        }
        CdmIndexService indexService = new CdmIndexService();
        indexService.setProject(project);
        try (Connection conn = indexService.openDbConnection()) {
            Statement stmt = conn.createStatement();
            // Query for all file objects. If the entry type is null, the object is a individual cdm object
            ResultSet rs = stmt.executeQuery("select count(*) from " + CdmIndexService.TB_NAME
                    + " where " + ENTRY_TYPE_FIELD + " = '" + ENTRY_TYPE_COMPOUND_CHILD + "'"
                    + " or " + ENTRY_TYPE_FIELD + " is null");
            indexedFileObjectsCountCache = rs.getInt(1);
            return indexedFileObjectsCountCache;
        } catch (SQLException e) {
            throw new MigrationException("Failed to determine number of objects", e);
        }
    }

    protected Map<String, Integer> countObjectsByType() {
        CdmIndexService indexService = new CdmIndexService();
        indexService.setProject(project);
        try (Connection conn = indexService.openDbConnection()) {
            Map<String, Integer> result = new HashMap<>();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select " + CdmIndexService.ENTRY_TYPE_FIELD + ", count(*)"
                    + " from " + CdmIndexService.TB_NAME
                    + " group by " + CdmIndexService.ENTRY_TYPE_FIELD);
            while (rs.next()) {
                result.put(rs.getString(1), new Integer(rs.getInt(2)));
            }
            return result;
        } catch (SQLException e) {
            throw new MigrationException("Failed to determine number of objects", e);
        }
    }

    private Set<String> objectIdSetCache;
    protected Set<String> getObjectIdSet() {
        if (objectIdSetCache != null) {
            return new HashSet<>(objectIdSetCache);
        }
        Set<String> ids = new HashSet<>();
        CdmIndexService indexService = new CdmIndexService();
        indexService.setProject(project);
        try (Connection conn = indexService.openDbConnection()) {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select " + CdmFieldInfo.CDM_ID + " from " + CdmIndexService.TB_NAME);
            while (rs.next()) {
                ids.add(rs.getString(1).trim());
            }
        } catch (SQLException e) {
            throw new MigrationException("Failed to determine number of objects", e);
        }
        objectIdSetCache = Collections.unmodifiableSet(ids);
        return new HashSet<>(objectIdSetCache);
    }

    protected CdmIndexService getIndexService() {
        if (indexService == null) {
            indexService = new CdmIndexService();
            indexService.setProject(project);
        }
        return indexService;
    }
}
