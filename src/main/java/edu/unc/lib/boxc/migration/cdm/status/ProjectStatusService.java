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
import java.time.Instant;

import com.google.common.collect.Iterators;

import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProjectProperties;
import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo;
import edu.unc.lib.boxc.migration.cdm.options.Verbosity;
import edu.unc.lib.boxc.migration.cdm.services.AccessFileService;
import edu.unc.lib.boxc.migration.cdm.services.CdmFieldService;
import edu.unc.lib.boxc.migration.cdm.services.DescriptionsService;
import edu.unc.lib.boxc.migration.cdm.services.SipService;
import edu.unc.lib.boxc.migration.cdm.services.SourceFileService;

/**
 * Service which displays overall the status of a migration project
 *
 * @author bbpennel
 */
public class ProjectStatusService extends AbstractStatusService {
    private CdmFieldService fieldService;

    public void report() {
        outputLogger.info("Status for project {}", project.getProjectName());

        MigrationProjectProperties properties = project.getProjectProperties();
        showField("Initialized", properties.getCreatedDate().toString());
        showField("User", properties.getCreator());
        showField("CDM Collection ID", properties.getCdmCollectionId());
        sectionDivider();

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
        sectionDivider();

        outputLogger.info("CDM Collection Exports");
        Instant exported = properties.getExportedDate();
        showField("Last Exported", exported == null ? "Not completed" : exported);
        if (exported == null) {
            return;
        }
        showField("Export files", countXmlDocuments(project.getExportPath()));
        sectionDivider();

        outputLogger.info("Index of CDM Objects");
        Instant indexed = properties.getIndexedDate();
        showField("Last Indexed", indexed == null ? "Not completed" : indexed);
        if (indexed == null) {
            return;
        }
        int totalObjects = countIndexedObjects();
        showField("Total Objects", totalObjects);
        sectionDivider();

        outputLogger.info("Destination Mappings");
        reportDestinationStats(totalObjects);
        sectionDivider();

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
        sectionDivider();

        outputLogger.info("Source File Mappings");
        Instant sourceUpdated = properties.getSourceFilesUpdatedDate();
        showField("Last Updated", sourceUpdated == null ? "Not completed" : sourceUpdated);
        if (sourceUpdated != null) {
            reportSourceMappings(totalObjects);
        }
        sectionDivider();

        outputLogger.info("Access File Mappings");
        Instant accessUpdated = properties.getAccessFilesUpdatedDate();
        showField("Last Updated", accessUpdated == null ? "Not completed" : accessUpdated);
        if (accessUpdated != null) {
            reportAccessMappings(totalObjects);
        }
        sectionDivider();

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

    private void reportDestinationStats(int totalObjects) {
        DestinationsStatusService destStatus = new DestinationsStatusService();
        destStatus.setProject(project);
        destStatus.reportDestinationStats(totalObjects, Verbosity.QUIET);
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
}
