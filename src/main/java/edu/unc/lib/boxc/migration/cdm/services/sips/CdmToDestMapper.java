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
import edu.unc.lib.boxc.migration.cdm.model.DestinationsInfo;

import java.util.HashMap;
import java.util.Map;

/**
 * Tracks mappings between CDM ids and destinations
 *
 * @author bbpennel
 */
public class CdmToDestMapper {
    private Map<String, DestinationSipEntry> cdmId2DestMap = new HashMap<>();

    public void put(DestinationsInfo.DestinationMapping destMapping, DestinationSipEntry destEntry) {
        cdmId2DestMap.put(destMapping.getId(), destEntry);
    }

    public DestinationSipEntry getDestinationEntry(String cdmId) {
        DestinationSipEntry entry = cdmId2DestMap.get(cdmId);
        if (entry == null) {
            return cdmId2DestMap.get(DestinationsInfo.DEFAULT_ID);
        } else {
            return entry;
        }
    }
}
