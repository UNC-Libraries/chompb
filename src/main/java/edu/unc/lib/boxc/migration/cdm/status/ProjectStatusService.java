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

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.Iterators;

import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.DestinationsInfo;
import edu.unc.lib.boxc.migration.cdm.model.DestinationsInfo.DestinationMapping;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProjectProperties;
import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo;
import edu.unc.lib.boxc.migration.cdm.services.AccessFileService;
import edu.unc.lib.boxc.migration.cdm.services.CdmFieldService;
import edu.unc.lib.boxc.migration.cdm.services.CdmIndexService;
import edu.unc.lib.boxc.migration.cdm.services.DescriptionsService;
import edu.unc.lib.boxc.migration.cdm.services.DestinationsService;
import edu.unc.lib.boxc.migration.cdm.services.SipService;
import edu.unc.lib.boxc.migration.cdm.services.SourceFileService;
import edu.unc.lib.boxc.migration.cdm.validators.DestinationsValidator;

/**
 * Service which displays overall the status of a migration project
 *
 * @author bbpennel
 */
public class ProjectStatusService {
    private static final String INDENT = "    ";
    private static final int MIN_LABEL_WIDTH = 20;

    private MigrationProject project;
    private CdmFieldService fieldService;

    public void report() {
        outputLogger.info("Status for project {}", project.getProjectName());

        MigrationProjectProperties properties = project.getProjectProperties();
        showField("Initialized", properties.getCreatedDate().toString());
        showField("User", properties.getCreator());
        showField("CDM Collection ID", properties.getCdmCollectionId());
        outputLogger.info("");

        outputLogger.info("CDM Collection Fields");
        fieldService = new CdmFieldService();
        try {
            fieldService.validateFieldsFile(project);
            showField("Mapping File Valid", "Yes");
            CdmFieldInfo fieldInfo = fieldService.loadFieldsFromProject(project);
            showField("Fields", fieldInfo.getFields().size());
            showField("Skipped", fieldInfo.getFields().stream().filter(f -> f.getSkipExport()).count());
        } catch (InvalidProjectStateException e) {
            showField("Mapping File Valid", "No (" + e.getMessage() + ")");
        } catch (IOException e) {
            throw new MigrationException("Failed to load fields", e);
        }
        outputLogger.info("");

        outputLogger.info("CDM Collection Exports");
        Instant exported = properties.getExportedDate();
        showField("Last Exported", exported == null ? "Not completed" : exported);
        if (exported == null) {
            return;
        }
        showField("Export files", countXmlDocuments(project.getExportPath()));
        outputLogger.info("");

        outputLogger.info("Index of CDM Objects");
        Instant indexed = properties.getIndexedDate();
        showField("Last Indexed", indexed == null ? "Not completed" : indexed);
        if (indexed == null) {
            return;
        }
        int totalObjects = countIndexedObjects();
        showField("Total Objects", totalObjects);
        outputLogger.info("");

        outputLogger.info("Destination Mappings");
        Instant destsGenerated = properties.getDestinationsGeneratedDate();
        showField("Last Generated", destsGenerated == null ? "Not completed" : destsGenerated);
        if (destsGenerated != null) {
            reportDestinationStats(totalObjects);
        }
        outputLogger.info("");

        outputLogger.info("Descriptions");
        DescriptionsService descService = new DescriptionsService();
        descService.setProject(project);
        showField("MODS Files", countXmlDocuments(project.getDescriptionsPath()));
        showField("New Collections MODS", countXmlDocuments(project.getNewCollectionDescriptionsPath()));
        Instant expanded = properties.getDescriptionsExpandedDate();
        showField("Last Expanded", expanded == null ? "Not completed" : expanded);
        try {
            showFieldWithPercent("Object MODS Records", descService.expandDescriptions(true), totalObjects);
        } catch (IOException e) {
            outputLogger.info("Failed to list MODS records: {}", e.getMessage());
        }
        outputLogger.info("");

        outputLogger.info("Source File Mappings");
        Instant sourceUpdated = properties.getSourceFilesUpdatedDate();
        showField("Last Updated", sourceUpdated == null ? "Not completed" : sourceUpdated);
        if (sourceUpdated != null) {
            reportSourceMappings(totalObjects);
        }
        outputLogger.info("");

        outputLogger.info("Access File Mappings");
        Instant accessUpdated = properties.getAccessFilesUpdatedDate();
        showField("Last Updated", accessUpdated == null ? "Not completed" : accessUpdated);
        if (accessUpdated != null) {
            reportAccessMappings(totalObjects);
        }
        outputLogger.info("");

        outputLogger.info("Submission Information Packages");
        Instant sipsGenerated = properties.getSipsGeneratedDate();
        showField("Last Generated", sipsGenerated == null ? "Not completed" : sipsGenerated);
        if (sipsGenerated != null) {
            SipService sipService = new SipService();
            sipService.setProject(project);
            showField("Number of SIPs", sipService.listSips().size());
            showField("SIPs Submitted", properties.getSipsSubmitted().size());
        }
    }

    private int countIndexedObjects() {
        CdmIndexService indexService = new CdmIndexService();
        indexService.setFieldService(fieldService);
        indexService.setProject(project);
        try (Connection conn = indexService.openDbConnection()) {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select count(*) from " + CdmIndexService.TB_NAME);
            return rs.getInt(1);
        } catch (SQLException e) {
            throw new MigrationException("Failed to determine number of objects", e);
        }
    }

    private void reportDestinationStats(int totalObjects) {
        try {
            DestinationsValidator validator = new DestinationsValidator();
            validator.setProject(project);
            int numErrors = validator.validateMappings(false).size();
            if (numErrors == 0) {
                showField("Destinations Valid", "Yes");
            } else {
                showField("Destinations Valid", "No (" + numErrors + " errors)");
                return;
            }

            DestinationsInfo destInfo = DestinationsService.loadMappings(project);
            int objsMapped = 0;
            int missingDest = 0;
            Set<String> dests = new HashSet<>();
            Set<String> newColls = new HashSet<>();
            boolean hasDefault = false;
            for (DestinationMapping destMapping : destInfo.getMappings()) {
                String dest = destMapping.getDestination();
                boolean isDefault = DestinationsInfo.DEFAULT_ID.equals(destMapping.getId());
                if (isDefault) {
                    hasDefault = dest != null;
                }
                if (dest != null) {
                    dests.add(dest + "|" + destMapping.getCollectionId());
                    if (!isDefault) {
                        objsMapped++;
                    }
                    if (!StringUtils.isBlank(destMapping.getCollectionId())) {
                        newColls.add(destMapping.getCollectionId());
                    }
                } else {
                    missingDest++;
                }
            }
            int totalMapped = hasDefault ? totalObjects - missingDest : objsMapped;
            showFieldWithPercent("Objects Mapped", totalMapped, totalObjects);
            int toDefault = hasDefault ? totalObjects - objsMapped : 0;
            showFieldWithPercent("To Default", toDefault, totalObjects);
            showField("Destinations", dests.size());
            showField("New Collections", newColls.size());
        } catch (IOException e) {
            outputLogger.info("Failed to load destinations mapping: {}", e.getMessage());
        }
    }

    private void reportSourceMappings(int totalObjects) {
        SourceFileService fileService = new SourceFileService();
        fileService.setProject(project);
        try {
            SourceFilesInfo info = fileService.loadMappings();
            long mappedCnt = info.getMappings().stream().filter(m -> m.getSourcePath() != null).count();
            showFieldWithPercent("Objects Mapped", (int) mappedCnt, totalObjects);
        } catch (IOException e) {
            outputLogger.info("Failed to source files mapping: {}", e.getMessage());
        }
    }

    private void reportAccessMappings(int totalObjects) {
        AccessFileService fileService = new AccessFileService();
        fileService.setProject(project);
        try {
            SourceFilesInfo info = fileService.loadMappings();
            long mappedCnt = info.getMappings().stream().filter(m -> m.getSourcePath() != null).count();
            showFieldWithPercent("Objects Mapped", (int) mappedCnt, totalObjects);
        } catch (IOException e) {
            outputLogger.info("Failed to access files mapping: {}", e.getMessage());
        }
    }

    private void showField(String label, Object value) {
        int padding = MIN_LABEL_WIDTH - label.length();
        outputLogger.info("{}{}: {}{}", INDENT, label, StringUtils.repeat(' ', padding), value);
    }

    private void showFieldWithPercent(String label, int value, int total) {
        int padding = MIN_LABEL_WIDTH - label.length();
        double percent = (double) value / total * 100;
        outputLogger.info("{}{}: {}{} ({}%)", INDENT, label, StringUtils.repeat(' ', padding),
                value, String.format("%.1f", percent));
    }

    private int countXmlDocuments(Path dirPath) {
        try (DirectoryStream<Path> pathStream = Files.newDirectoryStream(dirPath, "*.xml")) {
            return Iterators.size(pathStream.iterator());
        } catch (FileNotFoundException e) {
            return 0;
        } catch (IOException e) {
            outputLogger.info("Unable to count files for {}: {}", dirPath, e.getMessage());
            return 0;
        }
    }

    public void setProject(MigrationProject project) {
        this.project = project;
    }
}
