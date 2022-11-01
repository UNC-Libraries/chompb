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

import edu.unc.lib.boxc.common.xml.SecureXMLFactory;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.ChompbConfigService.ChompbConfig;
import edu.unc.lib.boxc.model.api.ResourceType;
import edu.unc.lib.boxc.model.api.xml.JDOMNamespaceUtil;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.StringUtils;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.slf4j.Logger;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Service for producing and managing the post migration report for verifying objects were successfully migrated.
 *
 * The report contains the original CDM URL, new Box-c info, and whether the destination object has been verified.
 *
 * This class is stateful and not thread safe.
 *
 * @author bbpennel
 */
public class PostMigrationReportService {
    private static final Logger log = getLogger(PostMigrationReportService.class);
    public static final String[] CSV_HEADERS = new String[] {
            "cdm_id", "cdm_url", "boxc_obj_type", "boxc_url", "boxc_title", "verified", "boxc_parent_work_url", "boxc_parent_work_title" };

    private MigrationProject project;
    private ChompbConfig chompbConfig;
    private DescriptionsService descriptionsService;
    private CSVPrinter csvPrinter;
    private SAXBuilder saxBuilder;
    private String cdmBaseUrl;
    private String singleBaseUrl;
    private String compoundBaseUrl;
    private String bxcBaseUrl;
    private String lastParentTitle;

    public void init() {
        this.project = project;
        var cdmEnv = chompbConfig.getCdmEnvironments().get(project.getProjectProperties().getCdmEnvironmentId());
        var baseWithoutPort = cdmEnv.getHttpBaseUrl().replaceFirst(":\\d+/", "/");
        this.cdmBaseUrl = baseWithoutPort + "cdm/";
        var collId = project.getProjectProperties().getCdmCollectionId();
        this.singleBaseUrl = cdmBaseUrl + "singleitem/collection/" + collId + "/id/";
        this.compoundBaseUrl = cdmBaseUrl + "compoundobject/collection/" + collId + "/id/";
        var bxcEnv = chompbConfig.getBxcEnvironments().get(project.getProjectProperties().getBxcEnvironmentId());
        this.bxcBaseUrl = bxcEnv.getHttpBaseUrl() + "record/";
        this.saxBuilder = SecureXMLFactory.createSAXBuilder();

        try {
            BufferedWriter writer = Files.newBufferedWriter(project.getPostMigrationReportPath());
            csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(CSV_HEADERS));
        } catch (IOException e) {
            throw new MigrationException("Error creating redirect mapping CSV", e);
        }
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

    public void addRow(String cdmObjectId, String boxcWorkId, String boxcFileId, boolean isSingleItem) {
        boolean isWorkObject = boxcFileId == null;
        String cdmUrl = buildCdmUrl(cdmObjectId, isWorkObject, isSingleItem);
        String cdmId = buildCdmId(cdmObjectId, isWorkObject, isSingleItem);
        String boxcTitle = extractTitle(cdmObjectId);
        String parentTitle, boxcUrl, parentUrl, objType;
        // file id is null, so the object is a work
        if (isWorkObject) {
            boxcUrl = this.bxcBaseUrl + boxcWorkId;
            parentUrl = null;
            parentTitle = null;
            objType = ResourceType.File.name();
            lastParentTitle = boxcTitle;
        } else {
            boxcUrl = this.bxcBaseUrl + boxcFileId;
            parentUrl = this.bxcBaseUrl + boxcWorkId;
            parentTitle = lastParentTitle;
            objType = ResourceType.Work.name();
        }
        try {
            csvPrinter.printRecord(cdmId, cdmUrl, objType, boxcUrl, boxcTitle, null, parentUrl, parentTitle);
        } catch (IOException e) {
            throw new MigrationException("Error adding row to redirect mapping CSV", e);
        }
    }

    private String buildCdmId(String cdmObjectId, boolean isWorkObject, boolean isSingleItem) {
        if (!isSingleItem) {
            return cdmObjectId;
        }
        return isWorkObject ? cdmObjectId : cdmObjectId + "/original_file";
    }

    private String buildCdmUrl(String cdmObjectId, boolean isWorkObject, boolean isSingleItem) {
        // no cdm id, so this is a grouped object and therefore does not have a cdm id
        if (StringUtils.isBlank(cdmObjectId)) {
            return null;
        }
        // Is a not a work object, or is the Work part of a single item object
        if (!isWorkObject || isSingleItem) {
            return this.singleBaseUrl + cdmObjectId;
        }
        // Is a compound object
        return this.compoundBaseUrl + cdmObjectId;
    }

    private String extractTitle(String cdmId) {
        if (StringUtils.isBlank(cdmId)) {
            return null;
        }
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
