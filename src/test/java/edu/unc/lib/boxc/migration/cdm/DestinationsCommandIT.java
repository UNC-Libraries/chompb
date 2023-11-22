package edu.unc.lib.boxc.migration.cdm;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import edu.unc.lib.boxc.migration.cdm.model.DestinationsInfo;
import edu.unc.lib.boxc.migration.cdm.model.DestinationsInfo.DestinationMapping;
import edu.unc.lib.boxc.migration.cdm.services.DestinationsService;
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
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author bbpennel
 */
@WireMockTest
public class DestinationsCommandIT extends AbstractCommandIT {
    private final static String DEST_UUID = "3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e";
    private final static String CUSTOM_DEST_ID = "8dd13ef6-1011-4acc-9f2f-ac1cdf03d800";
    private static final String SOLR_URL = "http:/localhost:8888/solr/test";

    @BeforeEach
    public void setup() throws Exception {
        initProjectAndHelper();
        setupChompbConfig();

        testHelper.getArchivalDestinationsService().setSolrServerUrl(SOLR_URL);
        //testHelper.getArchivalDestinationsService().setSolr(solrClient);
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
//        {
//            "responseHeader":{
//            "status":0,
//                    "QTime":0,
//                    "params":{
//                "q":"collectionId:20309 AND resourceType:Collection",
//                        "indent":"true",
//                        "fl":"id",
//                        "q.op":"OR"}},
//            "response":{"numFound":1,"start":0,"numFoundExact":true,"docs":[
//            {
//                "id":"3b467997-f94a-42c8-905f-d6cf7ead522c"}]
//        }}
//        stubFor(get(urlEqualTo("/solr"))
//                .willReturn(aResponse()
//                        .withHeader("status", "0")
//                        .withHeader("QTime", "0")
//                        .withHeader("q", "collectionId:20309 AND resourceType:Collection")
//                        .withHeader("indent", "true")
//                        .withHeader("fl", "id")
//                        .withHeader("q.op", "OR")
//                        .withBody("\"numFound\":1,\"start\":0,\"numFoundExact\":true," +
//                                "\"docs\":[{\"id\":\"3b467997-f94a-42c8-905f-d6cf7ead522c\"}]")));

        //groupa:group1
        stubFor((get(urlEqualTo("/solr"))
                .willReturn(okJson("{\"responseHeader\":{\"status\":0, \"QTime\":0, " +
                        "\"params\":{\"q\":\"groupa:group1 AND resourceType:Collection\", \"indent\":\"true\", " +
                        "\"fl\":\"id\", \"q.op\":\"OR\"}}," +
                        "\"response\":{\"numFound\":1,\"start\":0,\"numFoundExact\":true," +
                        "\"docs\":[{\"id\":\"8ae56bbc-400e-496d-af4b-3c585e20dba1\"}]}}"))));

        //groupa:group2
        stubFor((get(urlEqualTo("/solr"))
                .willReturn(okJson("{\"responseHeader\":{\"status\":0, \"QTime\":0, " +
                        "\"params\":{\"q\":\"groupa:group2 AND resourceType:Collection\", \"indent\":\"true\", " +
                        "\"fl\":\"id\", \"q.op\":\"OR\"}}," +
                        "\"response\":{\"numFound\":1,\"start\":0,\"numFoundExact\":true," +
                        "\"docs\":[{\"id\":\"bfe93126-849a-43a5-b9d9-391e18ffacc6\"}]}}"))));

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "--env-config", chompbConfigPath,
                "destinations", "map_archival_collections",
                "-n", "groupa"};
        executeExpectSuccess(args);

        assertArchivalCollectionMapping(DEST_UUID, "00123");
    }

    @Test
    public void generateArchivalDestNoFieldNameTest() throws Exception {
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "destinations", "map_archival_collections"};
        executeExpectFailure(args);
        assertOutputContains("Must provide a field name");
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
        DestinationMapping mapping1 = mappings.get(2);
        assertEquals("groupa:group2", mapping1.getId());
        DestinationMapping mapping2 = mappings.get(2);
        assertEquals("groupa:group1", mapping2.getId());
        DestinationMapping mapping3 = mappings.get(2);
        assertEquals(DestinationsInfo.DEFAULT_ID, mapping3.getId());
        assertEquals(defaultDest, mapping3.getDestination());
        assertEquals(defaultColl, mapping3.getCollectionId());
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
