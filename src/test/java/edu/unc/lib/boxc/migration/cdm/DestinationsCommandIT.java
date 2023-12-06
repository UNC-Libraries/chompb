package edu.unc.lib.boxc.migration.cdm;

import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import edu.unc.lib.boxc.migration.cdm.model.DestinationsInfo;
import edu.unc.lib.boxc.migration.cdm.model.DestinationsInfo.DestinationMapping;
import edu.unc.lib.boxc.migration.cdm.services.DestinationsService;
import edu.unc.lib.boxc.migration.cdm.test.BxcEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author bbpennel
 */
@WireMockTest(httpPort = BxcEnvironmentHelper.TEST_SOLR_PORT)
public class DestinationsCommandIT extends AbstractCommandIT {
    private final static String DEST_UUID = "3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e";
    private final static String CUSTOM_DEST_ID = "8dd13ef6-1011-4acc-9f2f-ac1cdf03d800";

    @BeforeEach
    public void setup() throws Exception {
        initProjectAndHelper();
        setupChompbConfig();
    }

    @Test
    public void generateNotIndexedTest() throws Exception {
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "destinations", "generate",
                "-dd", DEST_UUID};
        executeExpectFailure(args);

        assertOutputContains("Project must be indexed");
    }

    @Test
    public void generateWithNoOptionsTest() throws Exception {
        setIndexedDate();

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "destinations", "generate"};
        executeExpectFailure(args);
        assertOutputContains("Must provide a default destination");
    }

    @Test
    public void generateWithDefaultDestTest() throws Exception {
        setIndexedDate();

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "destinations", "generate",
                "-dd", DEST_UUID};
        executeExpectSuccess(args);

        assertDefaultMapping(DEST_UUID, "");
    }

    @Test
    public void generateWithDefaultDestAndCollTest() throws Exception {
        setIndexedDate();

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "destinations", "generate",
                "-dd", DEST_UUID,
                "-dc", "00123" };
        executeExpectSuccess(args);

        assertDefaultMapping(DEST_UUID, "00123");
    }

    @Test
    public void generateWithInvalidDefaultDestTest() throws Exception {
        setIndexedDate();

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "destinations", "generate",
                "-dd", "nothanks"};
        executeExpectFailure(args);
        assertOutputContains("must be a valid UUID");
    }

    @Test
    public void generateMappingExistsTest() throws Exception {
        setIndexedDate();

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "destinations", "generate",
                "-dd", DEST_UUID};
        executeExpectSuccess(args);
        assertDefaultMapping(DEST_UUID, "");

        // Should fail the second time and not update anything
        String[] args2 = new String[] {
                "-w", project.getProjectPath().toString(),
                "destinations", "generate",
                "-dd", DEST_UUID,
                "-dc", "00abc"};
        executeExpectFailure(args2);
        assertDefaultMapping(DEST_UUID, "");

        String[] args3 = new String[] {
                "-w", project.getProjectPath().toString(),
                "destinations", "generate",
                "-dd", DEST_UUID,
                "-dc", "00abc",
                "--force"};
        executeExpectSuccess(args3);
        assertDefaultMapping(DEST_UUID, "00abc");
    }

    @Test
    public void validateValidTest() throws Exception {
        setIndexedDate();

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "destinations", "generate",
                "-dd", DEST_UUID,
                "-dc", "00123" };
        executeExpectSuccess(args);

        String[] args2 = new String[] {
                "-w", project.getProjectPath().toString(),
                "destinations", "validate" };
        executeExpectSuccess(args2);

        assertOutputContains("PASS: Destination mapping at path " + project.getDestinationMappingsPath() + " is valid");
    }

    @Test
    public void validateInvalidTest() throws Exception {
        setIndexedDate();

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "destinations", "generate",
                "-dd", DEST_UUID,
                "-dc", "00123" };
        executeExpectSuccess(args);

        // Add a duplicate destination mapping
        FileUtils.write(project.getDestinationMappingsPath().toFile(),
                "25," + DEST_UUID + ",", StandardCharsets.UTF_8, true);

        String[] args2 = new String[] {
                "-w", project.getProjectPath().toString(),
                "destinations", "validate" };
        executeExpectFailure(args2);

        assertOutputContains("FAIL: Destination mapping at path " + project.getDestinationMappingsPath()
                + " is invalid");
        assertOutputContains("- Destination at line 3 has been previously mapped with a new collection");
        assertEquals(2, output.split("    - ").length, "Must only be two errors: " + output);
    }

    @Test
    public void statusValidTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "destinations", "generate",
                "-dd", DEST_UUID,
                "-dc", "00123" };
        executeExpectSuccess(args);

        String[] args2 = new String[] {
                "-w", project.getProjectPath().toString(),
                "destinations", "status" };
        executeExpectSuccess(args2);

        assertOutputMatches(".*Last Generated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Objects Mapped: +3 \\(100.0%\\).*");
        assertOutputMatches(".*Unmapped Objects: +0.*");
        assertOutputMatches(".*Destinations Valid: +Yes\n.*");
        assertOutputMatches(".*To Default: +3 \\(100.0%\\).*");
        assertOutputMatches(".*Destinations: +1\n.*");
        assertOutputMatches(".*Destinations:.*\n +\\* " + DEST_UUID + " 00123.*");
        assertOutputMatches(".*New Collections: +1\n.*");
        assertOutputMatches(".*New Collections:.*\n +\\* 00123.*");
    }

    @Test
    public void statusValidQuietTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "destinations", "generate",
                "-dd", DEST_UUID,
                "-dc", "00123" };
        executeExpectSuccess(args);

        String[] args2 = new String[] {
                "-w", project.getProjectPath().toString(),
                "destinations", "status",
                "-q" };
        executeExpectSuccess(args2);

        assertOutputMatches(".*Last Generated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Objects Mapped: +3 \\(100.0%\\).*");
        assertOutputNotMatches(".*Unmapped Objects: +0.*");
        assertOutputMatches(".*Destinations Valid: +Yes\n.*");
        assertOutputNotMatches(".*To Default: +3 \\(100.0%\\).*");
        assertOutputMatches(".*Destinations: +1\n.*");
        assertOutputNotMatches(".*\\* " + DEST_UUID + "\\|00123.*");
        assertOutputNotMatches(".*New Collections: +1\n.*");
        assertOutputNotMatches(".*\\* 00123.*");
    }

    @Test
    public void addSingleCdmIdCustomDestination() throws Exception {
        testHelper.indexExportData("mini_gilmer");

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "destinations", "generate",
                "-dd", DEST_UUID,
                "-dc", "00123" };
        executeExpectSuccess(args);

        String[] args2 = new String[] {
                "-w", project.getProjectPath().toString(),
                "destinations", "add",
                "-id", "25",
                "-dd", CUSTOM_DEST_ID};
        executeExpectSuccess(args2);

        assertCustomIdMappingAdded(getMappings(), "25", 1);
    }
    @Test
    public void addMultipleCdmIdCustomDestination() throws Exception {
        testHelper.indexExportData("mini_gilmer");

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "destinations", "generate",
                "-dd", DEST_UUID,
                "-dc", "00123" };
        executeExpectSuccess(args);

        String[] args2 = new String[] {
                "-w", project.getProjectPath().toString(),
                "destinations", "add",
                "-id", "25,26,27",
                "-dd", CUSTOM_DEST_ID};
        executeExpectSuccess(args2);

        var mappings = getMappings();
        assertMappingCount(mappings, 4);
        assertCustomIdMappingAdded(mappings, "25", 1);
        assertCustomIdMappingAdded(mappings, "26", 2);
        assertCustomIdMappingAdded(mappings, "27", 3);
    }

    @Test
    public void addCdmIdBeforeDestinationMappingExists() throws Exception {
        setIndexedDate();

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "destinations", "add",
                "-id", "25",
                "-dd", CUSTOM_DEST_ID};
        executeExpectFailure(args);

        assertOutputContains("FAIL: Destination mapping at path " + project.getDestinationMappingsPath()
                + " does not exist");
    }

    @Test
    public void addBlankCdmIdCustomDestinationMapping() throws Exception {
        setIndexedDate();

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "destinations", "generate",
                "-dd", DEST_UUID,
                "-dc", "00123" };
        executeExpectSuccess(args);

        String[] args2 = new String[] {
                "-w", project.getProjectPath().toString(),
                "destinations", "add",
                "-dd", CUSTOM_DEST_ID,
                "-id", ""};
        executeExpectFailure(args2);
        assertOutputContains("CDM ID must not be blank");
    }

    @Test
    public void statusValidVerboseTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "destinations", "generate",
                "-dd", DEST_UUID,
                "-dc", "00123" };
        executeExpectSuccess(args);

        String[] args2 = new String[] {
                "-w", project.getProjectPath().toString(),
                "destinations", "status",
                "-v" };
        executeExpectSuccess(args2);

        assertOutputMatches(".*Last Generated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Objects Mapped: +3 \\(100.0%\\).*");
        assertOutputMatches(".*Unmapped Objects: +0 \\(0.0%\\).*");
        assertOutputMatches(".*Destinations Valid: +Yes\n.*");
        assertOutputMatches(".*To Default: +3 \\(100.0%\\).*");
        assertOutputMatches(".*Destinations: +1\n.*");
        assertOutputMatches(".*Destinations:.*\n +\\* " + DEST_UUID + " 00123.*");
        assertOutputMatches(".*New Collections: +1\n.*");
        assertOutputMatches(".*New Collections:.*\n +\\* 00123.*");
    }

    @Test
    public void generateArchivalDestTest() throws Exception {
        testHelper.indexExportData("grouped_gilmer");
        //groupa:group1
        stubFor(get(urlEqualTo("/solr/select?q=collectionId%3Agroup1&fq=resourceType%3ACollection&wt=javabin&version=2"))
                .willReturn(aResponse()
                        .withBodyFile("arc_coll_resp_group1.bin")
                        .withHeader("Content-Type", "application/octet-stream")));
        //groupa:group2
        stubFor(get(urlEqualTo("/solr/select?q=collectionId%3Agroup2&fq=resourceType%3ACollection&wt=javabin&version=2"))
                .willReturn(aResponse()
                        .withBodyFile("arc_coll_resp_group2.bin")
                        .withHeader("Content-Type", "application/octet-stream")));

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "--env-config", chompbConfigPath,
                "destinations", "generate",
                "-ac",
                "-n", "groupa",
                "-dd", DEST_UUID,
                "-dc", "00123"};
        executeExpectSuccess(args);

        assertArchivalCollectionMapping(DEST_UUID, "00123");
    }

    @Test
    public void generateArchivalDestNoFieldNameTest() throws Exception {
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "--env-config", chompbConfigPath,
                "destinations", "generate",
                "--archival-collections"};
        executeExpectFailure(args);
        assertOutputContains("Must provide a field name");
    }

    @Test
    public void validateValidArchivalDestTest() throws Exception {
        testHelper.indexExportData("grouped_gilmer");
        //groupa:group1
        stubFor(get(urlEqualTo("/solr/select?q=collectionId%3Agroup1&fq=resourceType%3ACollection&wt=javabin&version=2"))
                .willReturn(aResponse()
                        .withBodyFile("arc_coll_resp_group1.bin")
                        .withHeader("Content-Type", "application/octet-stream")));
        //groupa:group2
        stubFor(get(urlEqualTo("/solr/select?q=collectionId%3Agroup2&fq=resourceType%3ACollection&wt=javabin&version=2"))
                .willReturn(aResponse()
                        .withBodyFile("arc_coll_resp_group2.bin")
                        .withHeader("Content-Type", "application/octet-stream")));

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "--env-config", chompbConfigPath,
                "destinations", "map_archival_collections",
                "-n", "groupa",
                "-dd", DEST_UUID,
                "-dc", "00123" };
        executeExpectSuccess(args);

        String[] args2 = new String[] {
                "-w", project.getProjectPath().toString(),
                "destinations", "validate" };
        executeExpectSuccess(args2);

        assertOutputContains("PASS: Destination mapping at path " + project.getDestinationMappingsPath() + " is valid");
    }

    @Test
    public void validateInvalidArchivalDestTest() throws Exception {
        testHelper.indexExportData("grouped_gilmer");
        //groupa:group1
        stubFor(get(urlEqualTo("/solr/select?q=collectionId%3Agroup1&fq=resourceType%3ACollection&wt=javabin&version=2"))
                .willReturn(aResponse()
                        .withBodyFile("arc_coll_resp_group1.bin")
                        .withHeader("Content-Type", "application/octet-stream")));
        //groupa:group2
        stubFor(get(urlEqualTo("/solr/select?q=collectionId%3Agroup2&fq=resourceType%3ACollection&wt=javabin&version=2"))
                .willReturn(aResponse()
                        .withBodyFile("arc_coll_resp_group2.bin")
                        .withHeader("Content-Type", "application/octet-stream")));

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "--env-config", chompbConfigPath,
                "destinations", "map_archival_collections",
                "-n", "groupa",
                "-dd", DEST_UUID,
                "-dc", "00123" };
        executeExpectSuccess(args);

        // Add a duplicate destination mapping
        FileUtils.write(project.getDestinationMappingsPath().toFile(),
                "groupa:," + DEST_UUID + ",", StandardCharsets.UTF_8, true);

        String[] args2 = new String[] {
                "-w", project.getProjectPath().toString(),
                "destinations", "validate" };
        executeExpectFailure(args2);

        assertOutputContains("FAIL: Destination mapping at path " + project.getDestinationMappingsPath()
                + " is invalid");
    }

    @Test
    public void validateInvalidFieldNameArchivalDestTest() throws Exception {
        testHelper.indexExportData("grouped_gilmer");
        //groupa:group1
        stubFor(get(urlEqualTo("/solr/select?q=collectionId%3Agroup1&fq=resourceType%3ACollection&wt=javabin&version=2"))
                .willReturn(aResponse()
                        .withBodyFile("arc_coll_resp_group1.bin")
                        .withHeader("Content-Type", "application/octet-stream")));
        //groupa:group2
        stubFor(get(urlEqualTo("/solr/select?q=collectionId%3Agroup2&fq=resourceType%3ACollection&wt=javabin&version=2"))
                .willReturn(aResponse()
                        .withBodyFile("arc_coll_resp_group2.bin")
                        .withHeader("Content-Type", "application/octet-stream")));

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "--env-config", chompbConfigPath,
                "destinations", "map_archival_collections",
                "-n", "groupa",
                "-dd", DEST_UUID,
                "-dc", "00123" };
        executeExpectSuccess(args);

        // Add a duplicate destination mapping
        FileUtils.write(project.getDestinationMappingsPath().toFile(),
                "groupa:test," + DEST_UUID + ",", StandardCharsets.UTF_8, true);

        String[] args2 = new String[] {
                "-w", project.getProjectPath().toString(),
                "destinations", "validate" };
        executeExpectFailure(args2);

        assertOutputContains("FAIL: Destination mapping at path " + project.getDestinationMappingsPath()
                + " is invalid");
    }

    private void assertDefaultMapping(String expectedDest, String expectedColl) throws Exception {
        var mappings = getMappings();
        assertMappingCount(mappings, 1);
        DestinationMapping mapping = mappings.get(0);
        assertEquals(DestinationsInfo.DEFAULT_ID, mapping.getId());
        assertEquals(expectedDest, mapping.getDestination());
        assertEquals(expectedColl, mapping.getCollectionId());
    }

    private void assertArchivalCollectionMapping(String defaultDest, String defaultColl) throws Exception {
        var mappings = getMappings();
        assertMappingCount(mappings, 3);
        DestinationMapping group2Mapping = mappings.get(0);
        assertEquals("groupa:group2", group2Mapping.getId());
        DestinationMapping group1Mapping = mappings.get(1);
        assertEquals("groupa:group1", group1Mapping.getId());
        DestinationMapping defaultMapping = mappings.get(2);
        assertEquals(DestinationsInfo.DEFAULT_ID, defaultMapping.getId());
        assertEquals(defaultDest, defaultMapping.getDestination());
        assertEquals(defaultColl, defaultMapping.getCollectionId());
    }

    private List<DestinationMapping> getMappings() throws IOException {
        DestinationsInfo info = DestinationsService.loadMappings(project);
        return info.getMappings();
    }

    private void assertCustomIdMappingAdded(List<DestinationMapping> mappings, String id, int index) {
        DestinationMapping mapping = mappings.get(index);
        assertEquals(id, mapping.getId());
        assertEquals(CUSTOM_DEST_ID, mapping.getDestination());
    }

    private void assertMappingCount(List<DestinationMapping> mappings, int count) {
        assertEquals(count, mappings.size());
    }

    private void setIndexedDate() throws Exception {
        project.getProjectProperties().setIndexedDate(Instant.now());
        ProjectPropertiesSerialization.write(project);
    }
}
