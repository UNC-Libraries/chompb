package edu.unc.lib.boxc.migration.cdm.services;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import edu.unc.lib.boxc.common.util.URIUtil;
import edu.unc.lib.boxc.common.xml.SecureXMLFactory;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.GroupMappingInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo;
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
    private SourceFileService sourceFileService;
    private CSVPrinter csvPrinter;
    private SAXBuilder saxBuilder;
    private String singleBaseUrl;
    private String compoundBaseUrl;
    private String bxcBaseUrl;
    private static final int CACHE_SIZE = 16;
    private Map<String, String> parentTitleCache;
    private SourceFilesInfo sourceFilesInfo;

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

        csvPrinter = openCsvPrinter();

        var mapBuilder = new ConcurrentLinkedHashMap.Builder<String, String>();
        mapBuilder.maximumWeightedCapacity(CACHE_SIZE);
        parentTitleCache = mapBuilder.build();
    }

    /**
     * @return newly opened printer for the report
     */
    public CSVPrinter openCsvPrinter() {
        try {
            BufferedWriter writer = Files.newBufferedWriter(project.getPostMigrationReportPath());
            return new CSVPrinter(writer, PostMigrationReportConstants.CSV_OUTPUT_FORMAT);
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
        String matchingValue = null;
        String parentUrl = null;
        String parentTitle = null;
        String objType = ResourceType.Work.name();
        String sourceFile = null;

        addRow(cdmObjectId, cdmUrl, objType, boxcUrl, boxcTitle, matchingValue, sourceFile,
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
        String matchingValue;
        String parentUrl = this.bxcBaseUrl + boxcWorkId;
        String parentTitle = getParentTitle(parentCdmId);
        String objType = ResourceType.File.name();
        String sourceFile;

        if (isSingleItem) {
            matchingValue = getMatchingValue(parentCdmId);
            sourceFile = getSourceFile(parentCdmId);
        } else {
            matchingValue = getMatchingValue(fileCdmId);
            sourceFile = getSourceFile(fileCdmId);
        }

        addRow(fileCdmId, cdmUrl, objType, boxcUrl, boxcTitle, matchingValue, sourceFile,
                null, parentUrl, parentTitle, null);
    }

    protected void addRow(String cdmId, String cdmUrl, String objType, String boxcUrl, String boxcTitle,
                          String matchingValue, String sourceFile, String verified, String parentUrl,
                          String parentTitle, Integer childCount) throws IOException {
        csvPrinter.printRecord(cdmId, cdmUrl, objType, boxcUrl, boxcTitle, matchingValue, sourceFile,
                verified, parentUrl, parentTitle, childCount);
    }

    private String buildCdmUrl(String cdmObjectId, boolean isWorkObject, boolean isSingleItem) {
        // grouped object and therefore does not have a cdm id
        if (cdmObjectId.startsWith(GroupMappingInfo.GROUPED_WORK_PREFIX)) {
            return null;
        }
        // Is not a work object, or the Work is part of a single item object
        if (!isWorkObject || isSingleItem) {
            return this.singleBaseUrl + cdmObjectId.replace("/original_file", "");
        }
        // Is a compound object
        return this.compoundBaseUrl + cdmObjectId;
    }

    // Get the title of the parent object, using a cache to prevent needing to read its MODS for every child
    private String getParentTitle(String cdmId) {
        if (parentTitleCache.containsKey(cdmId)) {
            return parentTitleCache.get(cdmId);
        }
        String title = extractTitle(cdmId);
        parentTitleCache.put(cdmId, title);
        return title;
    }

    // Get the mods:title of the object with the provided cdm id by extracting it from the associated MODS document
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

    private String getMatchingValue(String cdmId) throws IOException {
        var sourceFilesInfo = getSourceFilesInfo();
        String matchingValue = sourceFilesInfo.getMappingByCdmId(cdmId).getMatchingValue();
        return matchingValue;
    }

    private String getSourceFile(String cdmId) throws IOException {
        var sourceFilesInfo = getSourceFilesInfo();
        String sourceFile = sourceFilesInfo.getMappingByCdmId(cdmId).getSourcePathString();
        return sourceFile;
    }

    private SourceFilesInfo getSourceFilesInfo() throws IOException {
        if (sourceFilesInfo == null) {
            sourceFilesInfo = sourceFileService.loadMappings();
        }
        return sourceFilesInfo;
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

    public void setSourceFileService(SourceFileService sourceFileService) {
        this.sourceFileService = sourceFileService;
    }
}
