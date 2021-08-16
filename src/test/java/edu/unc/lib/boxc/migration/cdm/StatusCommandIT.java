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
package edu.unc.lib.boxc.migration.cdm;

import static java.nio.charset.StandardCharsets.US_ASCII;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.SipGenerationOptions;
import edu.unc.lib.boxc.migration.cdm.options.SourceFileMappingOptions;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.services.SipService;
import edu.unc.lib.boxc.migration.cdm.test.SipServiceHelper;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;

/**
 * @author bbpennel
 */
public class StatusCommandIT extends AbstractCommandIT {
    private final static String COLLECTION_ID = "my_coll";
    private final static String PROJECT_ID = "my_proj";
    private final static String DEST_UUID = "3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e";

    private MigrationProject project;
    private SipServiceHelper testHelper;

    @Before
    public void setup() throws Exception {
        project = MigrationProjectFactory.createMigrationProject(
                baseDir, PROJECT_ID, COLLECTION_ID, USERNAME);
        testHelper = new SipServiceHelper(project, tmpFolder.newFolder().toPath());
    }

    @Test
    public void reportUninitalized() throws Exception {
        String[] args = new String[] {
                "-w", baseDir.toString(),
                "status" };
        executeExpectFailure(args);

        assertOutputContains("does not contain an initialized project");
    }

    @Test
    public void reportInitalized() throws Exception {
        Files.copy(Paths.get("src/test/resources/gilmer_fields.csv"), project.getFieldsPath());

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "status" };
        executeExpectSuccess(args);

        assertOutputContains("Status for project " + PROJECT_ID);
        assertOutputMatches(".*Initialized: +[0-9\\-T:]+.*");
        assertOutputMatches(".*User: +theuser.*");
        assertOutputMatches(".*CDM Collection ID: +" + COLLECTION_ID + ".*");

        assertOutputContains("CDM Collection Fields");
        assertOutputMatches(".*Mapping File Valid: +Yes.*");
        assertOutputMatches(".*Fields: +60\n.*");
        assertOutputMatches(".*Skipped: +1\n.*");

        assertOutputContains("CDM Collection Exports");
        assertOutputMatches(".*Last Exported: +Not completed.*");
    }

    @Test
    public void reportInvalidFields() throws Exception {
        FileUtils.write(project.getFieldsPath().toFile(), "hmm nope", US_ASCII);

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "status" };
        executeExpectSuccess(args);

        assertOutputContains("Status for project " + PROJECT_ID);
        assertOutputMatches(".*Initialized: +[0-9\\-T:]+.*");
        assertOutputMatches(".*User: +theuser.*");
        assertOutputMatches(".*CDM Collection ID: +" + COLLECTION_ID + ".*");

        assertOutputContains("CDM Collection Fields");
        assertOutputMatches(".*Mapping File Valid: +No.*");

        assertOutputContains("CDM Collection Exports");
        assertOutputMatches(".*Last Exported: +Not completed.*");
    }

    @Test
    public void reportExported() throws Exception {
        Instant exported = Instant.now();
        Files.copy(Paths.get("src/test/resources/gilmer_fields.csv"), project.getFieldsPath());
            Files.copy(Paths.get("src/test/resources/sample_exports/export_1.xml"),
                    project.getExportPath().resolve("export_1.xml"));
        project.getProjectProperties().setExportedDate(exported);
        ProjectPropertiesSerialization.write(project);

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "status" };
        executeExpectSuccess(args);

        assertOutputContains("Status for project " + PROJECT_ID);

        assertOutputContains("CDM Collection Fields");

        assertOutputContains("CDM Collection Exports");
        assertOutputMatches(".*Last Exported: +" + exported + ".*");
        assertOutputMatches(".*Export files: +1\n.*");

        assertOutputContains("Index of CDM Objects");
        assertOutputMatches(".*Last Indexed: +Not completed.*");
    }

    @Test
    public void reportIndexed() throws Exception {
        testHelper.indexExportData("export_1.xml");

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "status" };
        executeExpectSuccess(args);

        assertOutputContains("Status for project " + PROJECT_ID);

        assertOutputContains("CDM Collection Fields");
        assertOutputContains("CDM Collection Exports");

        assertOutputContains("Index of CDM Objects");
        assertOutputMatches(".*Last Indexed: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Total Objects: +3\n.*");

        assertOutputContains("Descriptions");
        assertOutputMatches(".*MODS Files: +0.*");
        assertOutputMatches(".*New Collections MODS: +0.*");
        assertOutputMatches(".*Last Expanded: +Not completed.*");
        assertOutputMatches(".*Object MODS Records: +0 \\(0.0%\\).*");

        assertOutputMatches(".*Destination Mappings\n +Last Generated: +Not completed.*");
        assertOutputMatches(".*Source File Mappings\n +Last Updated: +Not completed.*");
        assertOutputMatches(".*Access File Mappings\n +Last Updated: +Not completed.*");
        assertOutputMatches(".*Submission Information Packages\n +Last Generated: +Not completed.*");
    }

    @Test
    public void reportMixedDestinations() throws Exception {
        testHelper.indexExportData("export_1.xml");
        String newCollId = "00123test";
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, newCollId);
        FileUtils.write(project.getDestinationMappingsPath().toFile(),
                "26,bc54e3b5-48e1-4665-90b3-ee8b05721706,", StandardCharsets.US_ASCII, true);

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "status" };
        executeExpectSuccess(args);

        assertOutputContains("Status for project " + PROJECT_ID);

        assertOutputContains("CDM Collection Fields");
        assertOutputContains("CDM Collection Exports");

        assertOutputContains("Index of CDM Objects");
        assertOutputMatches(".*Total Objects: +3\n.*");

        assertOutputContains("Descriptions");

        assertOutputMatches(".*Destination Mappings\n +Last Generated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Destination Mappings\n.*\n +Objects Mapped: +3 \\(100.0%\\).*");
        assertOutputMatches(".*To Default: +2 \\(66.7%\\).*");
        assertOutputMatches(".*Destinations: +2\n.*");
        assertOutputMatches(".*New Collections: +1\n.*");

        assertOutputMatches(".*Source File Mappings\n +Last Updated: +Not completed.*");
        assertOutputMatches(".*Access File Mappings\n +Last Updated: +Not completed.*");
        assertOutputMatches(".*Submission Information Packages\n +Last Generated: +Not completed.*");
    }

    @Test
    public void reportSourceFilesPartiallyMapped() throws Exception {
        testHelper.indexExportData("export_1.xml");
        testHelper.addSourceFile("276_182_E.tif");
        SourceFileMappingOptions opts = testHelper.makeSourceFileOptions(testHelper.getSourceFilesBasePath());
        testHelper.getSourceFileService().generateMapping(opts);

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "status" };
        executeExpectSuccess(args);

        assertOutputContains("Status for project " + PROJECT_ID);

        assertOutputContains("CDM Collection Fields");
        assertOutputContains("CDM Collection Exports");

        assertOutputContains("Index of CDM Objects");
        assertOutputMatches(".*Total Objects: +3\n.*");

        assertOutputContains("Descriptions");

        assertOutputMatches(".*Destination Mappings\n +Last Generated: +Not completed.*");

        assertOutputMatches(".*Source File Mappings\n +Last Updated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Source File Mappings\n.*\n +Objects Mapped: +1 \\(33.3%\\).*");

        assertOutputMatches(".*Access File Mappings\n +Last Updated: +Not completed.*");
        assertOutputMatches(".*Submission Information Packages\n +Last Generated: +Not completed.*");
    }

    @Test
    public void reportAccessFilesPartiallyMapped() throws Exception {
        testHelper.indexExportData("export_1.xml");
        testHelper.populateSourceFiles("276_182_E.tif", "276_183B_E.tif", "276_203_E.tif");
        testHelper.addAccessFile("276_182_E.tif");
        SourceFileMappingOptions opts = testHelper.makeSourceFileOptions(testHelper.getAccessFilesBasePath());
        testHelper.getAccessFileService().generateMapping(opts);

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "status" };
        executeExpectSuccess(args);

        assertOutputContains("Status for project " + PROJECT_ID);

        assertOutputContains("CDM Collection Fields");
        assertOutputContains("CDM Collection Exports");

        assertOutputContains("Index of CDM Objects");
        assertOutputMatches(".*Total Objects: +3\n.*");

        assertOutputContains("Descriptions");

        assertOutputMatches(".*Destination Mappings\n +Last Generated: +Not completed.*");

        assertOutputMatches(".*Source File Mappings\n +Last Updated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Source File Mappings\n.*\n +Objects Mapped: +3 \\(100.0%\\).*");

        assertOutputMatches(".*Access File Mappings\n +Last Updated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Access File Mappings\n.*\n +Objects Mapped: +1 \\(33.3%\\).*");

        assertOutputMatches(".*Submission Information Packages\n +Last Generated: +Not completed.*");
    }

    @Test
    public void reportSipGeneratedWithNewCollection() throws Exception {
        testHelper.indexExportData("export_1.xml");
        String newCollId = "00123test";
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, newCollId);
        testHelper.populateDescriptions("gilmer_mods1.xml");
        Files.createFile(project.getNewCollectionDescriptionsPath().resolve(newCollId + ".xml"));
        testHelper.populateSourceFiles("276_182_E.tif", "276_183B_E.tif", "276_203_E.tif");
        SipService sipService = testHelper.createSipsService();
        SipGenerationOptions sipOpts = new SipGenerationOptions();
        sipOpts.setUsername(USERNAME);
        sipService.generateSips(sipOpts);

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "status" };
        executeExpectSuccess(args);

        assertOutputContains("Status for project " + PROJECT_ID);

        assertOutputContains("CDM Collection Fields");
        assertOutputContains("CDM Collection Exports");

        assertOutputContains("Index of CDM Objects");
        assertOutputMatches(".*Total Objects: +3\n.*");

        assertOutputContains("Descriptions");
        assertOutputMatches(".*MODS Files: +1\n.*");
        assertOutputMatches(".*New Collections MODS: +1\n.*");
        assertOutputMatches(".*Last Expanded: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Object MODS Records: +3 \\(100.0%\\).*");

        assertOutputMatches(".*Destination Mappings\n +Last Generated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Destination Mappings\n.*\n +Objects Mapped: +3 \\(100.0%\\).*");
        assertOutputMatches(".*To Default: +3 \\(100.0%\\).*");
        assertOutputMatches(".*Destinations: +1\n.*");
        assertOutputMatches(".*New Collections: +1\n.*");

        assertOutputMatches(".*Source File Mappings\n +Last Updated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Source File Mappings\n.*\n +Objects Mapped: +3 \\(100.0%\\).*");

        assertOutputMatches(".*Access File Mappings\n +Last Updated: +Not completed.*");
        assertOutputMatches(".*Submission Information Packages\n +Last Generated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Number of SIPs: +1\n.*");
        assertOutputMatches(".*SIPs Submitted: +0\n.*");
    }
}
