package edu.unc.lib.boxc.migration.cdm;

import edu.unc.lib.boxc.migration.cdm.options.SipGenerationOptions;
import edu.unc.lib.boxc.migration.cdm.options.SourceFileMappingOptions;
import edu.unc.lib.boxc.migration.cdm.services.CdmFileRetrievalService;
import edu.unc.lib.boxc.migration.cdm.services.SipService;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

/**
 * @author bbpennel
 */
public class StatusCommandIT extends AbstractCommandIT {
    private final static String DEST_UUID = "3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e";

    @Before
    public void setup() throws Exception {
        initProjectAndHelper();
    }

    @Test
    public void reportUninitialized() throws Exception {
        String[] args = new String[] {
                "-w", baseDir.toString(),
                "status" };
        executeExpectFailure(args);

        assertOutputContains("does not contain an initialized project");
    }

    @Test
    public void reportInitialized() throws Exception {
        Files.copy(Paths.get("src/test/resources/gilmer_fields.csv"), project.getFieldsPath());

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "status" };
        executeExpectSuccess(args);

        assertOutputContains("Status for project " + PROJECT_ID);
        assertOutputMatches(".*Initialized: +[0-9\\-T:]+.*");
        assertOutputMatches(".*User: +theuser.*");
        assertOutputMatches(".*CDM Collection ID: +" + defaultCollectionId + ".*");

        assertOutputContains("CDM Collection Fields");
        assertOutputMatches(".*Mapping File Valid: +Yes.*");
        assertOutputMatches(".*Fields: +61\n.*");
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
        assertOutputMatches(".*CDM Collection ID: +" + defaultCollectionId + ".*");

        assertOutputContains("CDM Collection Fields");
        assertOutputMatches(".*Mapping File Valid: +No.*");

        assertOutputContains("CDM Collection Exports");
        assertOutputMatches(".*Last Exported: +Not completed.*");
    }

    @Test
    public void reportExported() throws Exception {
        Instant exported = Instant.now();
        Files.copy(Paths.get("src/test/resources/gilmer_fields.csv"), project.getFieldsPath());
        Files.copy(Paths.get("src/test/resources/descriptions/mini_gilmer/index/description/desc.all"),
                CdmFileRetrievalService.getDescAllPath(project), REPLACE_EXISTING);
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

        assertOutputContains("Index of CDM Objects");
        assertOutputMatches(".*Last Indexed: +Not completed.*");
    }

    @Test
    public void reportIndexed() throws Exception {
        testHelper.indexExportData("mini_gilmer");

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
        assertOutputMatches(".*Single Objects: +3 .*");

        assertOutputContains("Descriptions");
        assertOutputMatches(".*MODS Files: +0.*");
        assertOutputMatches(".*New Collections MODS: +0.*");
        assertOutputMatches(".*Last Expanded: +Not completed.*");
        assertOutputMatches(".*Object MODS Records: +0 \\(0.0%\\).*");

        assertOutputMatches(".*Destination Mappings\n +Last Generated: +Not completed.*");
        assertOutputMatches(".*Source File Mappings\n +Last Updated: +Not completed.*");
        assertOutputMatches(".*Access File Mappings\n +Last Updated: +Not completed.*");
        assertOutputMatches(".*Grouped Object Mappings\n +Last Generated: +Not completed.*");
        assertOutputMatches(".*Submission Information Packages\n +Last Generated: +Not completed.*");
    }

    @Test
    public void reportMixedDestinations() throws Exception {
        testHelper.indexExportData("mini_gilmer");
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
        assertOutputMatches(".*Destinations Valid: +Yes\n.*");
        assertOutputMatches(".*Destinations: +2\n.*");

        assertOutputMatches(".*Source File Mappings\n +Last Updated: +Not completed.*");
        assertOutputMatches(".*Access File Mappings\n +Last Updated: +Not completed.*");
        assertOutputMatches(".*Submission Information Packages\n +Last Generated: +Not completed.*");
    }

    @Test
    public void reportDestinationsInvalid() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        String newCollId = "00123test";
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, newCollId);
        FileUtils.write(project.getDestinationMappingsPath().toFile(),
                "26,abdcdfg,", StandardCharsets.US_ASCII, true);

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
        assertOutputMatches(".*Destinations Valid: +No \\(1 error.*");

        assertOutputMatches(".*Source File Mappings\n +Last Updated: +Not completed.*");
        assertOutputMatches(".*Access File Mappings\n +Last Updated: +Not completed.*");
        assertOutputMatches(".*Submission Information Packages\n +Last Generated: +Not completed.*");
    }

    @Test
    public void reportSourceFilesPartiallyMapped() throws Exception {
        testHelper.indexExportData("mini_gilmer");
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
        testHelper.indexExportData("mini_gilmer");
        testHelper.populateSourceFiles("276_182_E.tif", "276_183_E.tif", "276_203_E.tif");
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
        testHelper.indexExportData("mini_gilmer");
        String newCollId = "00123test";
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, newCollId);
        testHelper.populateDescriptions("gilmer_mods1.xml");
        Files.createFile(project.getNewCollectionDescriptionsPath().resolve(newCollId + ".xml"));
        testHelper.populateSourceFiles("276_182_E.tif", "276_183_E.tif", "276_203_E.tif");
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
        assertOutputMatches(".*Destinations: +1\n.*");

        assertOutputMatches(".*Source File Mappings\n +Last Updated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Source File Mappings\n.*\n +Objects Mapped: +3 \\(100.0%\\).*");

        assertOutputMatches(".*Access File Mappings\n +Last Updated: +Not completed.*");
        assertOutputMatches(".*Submission Information Packages\n +Last Generated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Number of SIPs: +1\n.*");
        assertOutputMatches(".*SIPs Submitted: +0\n.*");
    }

    @Test
    public void reportSipGeneratedWithCompoundObjects() throws Exception {
        testHelper.indexExportData(Paths.get("src/test/resources/keepsakes_fields.csv"), "mini_keepsakes");
        String newCollId = "00123test";
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, null);
        testHelper.getDescriptionsService().generateDocuments(true);
        testHelper.getDescriptionsService().expandDescriptions();
        var sourceOptions = testHelper.makeSourceFileOptions(testHelper.getSourceFilesBasePath());
        sourceOptions.setExportField("filena");
        testHelper.populateSourceFiles(sourceOptions, "nccg_ck_09.tif", "nccg_ck_1042-22_v1.tif",
                "nccg_ck_1042-22_v2.tif", "nccg_ck_549-4_v1.tif", "nccg_ck_549-4_v2.tif");
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
        assertOutputMatches(".*Total Objects: +7\n.*");
        assertOutputMatches(".*Single Objects: +1 .*");
        assertOutputMatches(".*cpd_object: +2 .*");
        assertOutputMatches(".*cpd_child: +4 .*");

        assertOutputContains("Descriptions");
        assertOutputMatches(".*MODS Files: +1\n.*");
        assertOutputMatches(".*Last Expanded: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Object MODS Records: +7 \\(100.0%\\).*");

        assertOutputMatches(".*Destination Mappings\n +Last Generated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Destination Mappings\n.*\n +Objects Mapped: +7 \\(100.0%\\).*");
        assertOutputMatches(".*Destinations: +1\n.*");

        assertOutputMatches(".*Source File Mappings\n +Last Updated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Source File Mappings\n.*\n +Objects Mapped: +5 \\(71.4%\\).*");

        assertOutputMatches(".*Access File Mappings\n +Last Updated: +Not completed.*");
        assertOutputMatches(".*Submission Information Packages\n +Last Generated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Number of SIPs: +1\n.*");
        assertOutputMatches(".*SIPs Submitted: +0\n.*");
    }

    @Test
    public void reportDescriptionNotExpanded() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        String newCollId = "00123test";
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, newCollId);
        Files.copy(Paths.get("src/test/resources/mods_collections/gilmer_mods1.xml"),
                project.getDescriptionsPath().resolve("gilmer_mods1.xml"));

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "status" };
        executeExpectSuccess(args);

        assertOutputContains("Descriptions");
        assertOutputMatches(".*MODS Files: +1\n.*");
        assertOutputMatches(".*Last Expanded: +Not completed.*");
        assertOutputMatches(".*Object MODS Records: +3 \\(100.0%\\).*");
    }
}
