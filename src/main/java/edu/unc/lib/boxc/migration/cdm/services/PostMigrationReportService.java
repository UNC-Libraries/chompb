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
package edu.unc.lib.boxc.migration.cdm.services;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import edu.unc.lib.boxc.common.util.URIUtil;
import edu.unc.lib.boxc.common.xml.SecureXMLFactory;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.GroupMappingInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.ChompbConfigService.ChompbConfig;
import edu.unc.lib.boxc.migration.cdm.util.PostMigrationReportConstants;
import edu.unc.lib.boxc.model.api.ResourceType;
import edu.unc.lib.boxc.model.api.xml.JDOMNamespaceUtil;
import org.apache.commons.csv.CSVPrinter;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.slf4j.Logger;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Service for producing and managing the post migration report for verifying objects were successfully migrated.
 *
 * The report contains the original CDM URL, new Box-c info, and whether the destination object has been verified.
 *
 * @author bbpennel
 */
public class PostMigrationReportService {
    private static final Logger log = getLogger(PostMigrationReportService.class);

    private MigrationProject project;
    private ChompbConfig chompbConfig;
    private DescriptionsService descriptionsService;
    private CSVPrinter csvPrinter;
    private SAXBuilder saxBuilder;
    private String singleBaseUrl;
    private String compoundBaseUrl;
    private String bxcBaseUrl;
    private static final int CACHE_SIZE = 16;
    private Map<String, String> parentTitleCache;

    /**
     * Initialize the service
     */
    public void init() {
        var cdmEnv = chompbConfig.getCdmEnvironments().get(project.getProjectProperties().getCdmEnvironmentId());
        var baseWithoutPort = cdmEnv.getHttpBaseUrl().replaceFirst(":\\d+", "");
        var collId = project.getProjectProperties().getCdmCollectionId();
        this.singleBaseUrl = URIUtil.join(baseWithoutPort, "cdm/singleitem/collection", collId, "id") + "/";
        this.compoundBaseUrl = URIUtil.join(baseWithoutPort, "cdm/compoundobject/collection", collId, "id") + "/";
        var bxcEnv = chompbConfig.getBxcEnvironments().get(project.getProjectProperties().getBxcEnvironmentId());
        this.bxcBaseUrl = URIUtil.join(bxcEnv.getHttpBaseUrl(), "record") + "/";
        this.saxBuilder = SecureXMLFactory.createSAXBuilder();

        try {
            BufferedWriter writer = Files.newBufferedWriter(project.getPostMigrationReportPath());
            csvPrinter = new CSVPrinter(writer, PostMigrationReportConstants.CSV_OUTPUT_FORMAT);
        } catch (IOException e) {
            throw new MigrationException("Error creating redirect mapping CSV", e);
        }

        var mapBuilder = new ConcurrentLinkedHashMap.Builder<String, String>();
        mapBuilder.maximumWeightedCapacity(CACHE_SIZE);
        parentTitleCache = mapBuilder.build();
    }

    /**
     * Closes report CSV
     */
    public void closeCsv() {
        try {
            csvPrinter.close();
        } catch (IOException e) {
            throw new MigrationException("Error closing post migration report CSV", e);
        }
    }

    /**
     * Add a report entry for a work object
     * @param cdmObjectId cdm id of the work
     * @param boxcWorkId boxc uuid of the work
     * @param childCount number of children for the work
     * @param isSingleItem whether this work is a CDM single item type
     * @throws IOException
     */
    public void addWorkRow(String cdmObjectId, String boxcWorkId, int childCount, boolean isSingleItem)
            throws IOException {
        String cdmUrl = buildCdmUrl(cdmObjectId, true, isSingleItem);
        String boxcTitle = getParentTitle(cdmObjectId);
        String boxcUrl = this.bxcBaseUrl + boxcWorkId;
        String parentUrl = null;
        String parentTitle = null;
        String objType = ResourceType.Work.name();
        csvPrinter.printRecord(cdmObjectId, cdmUrl, objType, boxcUrl, boxcTitle,
                null, parentUrl, parentTitle, childCount);
    }

    /**
     * Add a report entry for a file object
     * @param fileCdmId cdm id of the file
     * @param parentCdmId  cdm id of the parent object
     * @param boxcWorkId bxc uuid of the work containing this file
     * @param boxcFileId bxc uuid of the file object
     * @param isSingleItem whether the work containing this file is a CDM single item
     * @throws IOException
     */
    public void addFileRow(String fileCdmId, String parentCdmId, String boxcWorkId, String boxcFileId,
                           boolean isSingleItem)
            throws IOException {
        String cdmUrl = buildCdmUrl(fileCdmId, false, isSingleItem);
        String boxcTitle = extractTitle(fileCdmId);
        String boxcUrl = this.bxcBaseUrl + boxcFileId;
        String parentUrl = this.bxcBaseUrl + boxcWorkId;
        String parentTitle = getParentTitle(parentCdmId);
        String objType = ResourceType.File.name();
        csvPrinter.printRecord(fileCdmId, cdmUrl, objType, boxcUrl, boxcTitle,
                null, parentUrl, parentTitle, null);
    }

    private String buildCdmUrl(String cdmObjectId, boolean isWorkObject, boolean isSingleItem) {
        // grouped object and therefore does not have a cdm id
        if (cdmObjectId.startsWith(GroupMappingInfo.GROUPED_WORK_PREFIX)) {
            return null;
        }
        // Is a not a work object, or is the Work part of a single item object
        if (!isWorkObject || isSingleItem) {
            return this.singleBaseUrl + cdmObjectId.replace("/original_file", "");
        }
        // Is a compound object
        return this.compoundBaseUrl + cdmObjectId;
    }

    private String getParentTitle(String cdmId) {
        if (parentTitleCache.containsKey(cdmId)) {
            return parentTitleCache.get(cdmId);
        }
        String title = extractTitle(cdmId);
        parentTitleCache.put(cdmId, title);
        return title;
    }

    private String extractTitle(String cdmId) {
        var descPath = descriptionsService.getExpandedDescriptionFilePath(cdmId);
        if (Files.notExists(descPath)) {
            return null;
        }
        try {
            var doc = saxBuilder.build(descPath.toFile());
            var titleInfo = doc.getRootElement().getChild("titleInfo", JDOMNamespaceUtil.MODS_V3_NS);
            if (titleInfo == null) {
                return null;
            }
            return titleInfo.getChildTextTrim("title", JDOMNamespaceUtil.MODS_V3_NS);
        } catch (JDOMException | IOException e) {
            log.error("Failed to read MODS file for {} while extracting title for post migration report: {}",
                    cdmId, e.getMessage());
        }
        return null;
    }

    public void setProject(MigrationProject project) {
        this.project = project;
    }

    public void setChompbConfig(ChompbConfig chompbConfig) {
        this.chompbConfig = chompbConfig;
    }

    public void setDescriptionsService(DescriptionsService descriptionsService) {
        this.descriptionsService = descriptionsService;
    }
}
