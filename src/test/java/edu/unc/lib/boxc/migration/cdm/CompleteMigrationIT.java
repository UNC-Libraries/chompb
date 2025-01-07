package edu.unc.lib.boxc.migration.cdm;

import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import edu.unc.lib.boxc.deposit.api.RedisWorkerConstants.DepositField;
import edu.unc.lib.boxc.deposit.impl.model.DepositDirectoryManager;
import edu.unc.lib.boxc.deposit.impl.model.DepositStatusFactory;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.MigrationSip;
import edu.unc.lib.boxc.migration.cdm.services.CdmFieldService;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.SipServiceHelper;
import edu.unc.lib.boxc.migration.cdm.test.TestSshServer;
import edu.unc.lib.boxc.model.api.rdf.Cdr;
import edu.unc.lib.boxc.model.fcrepo.ids.PIDs;
import edu.unc.lib.boxc.persist.api.PackagingType;
import org.apache.commons.io.IOUtils;
import org.apache.jena.rdf.model.Bag;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static edu.unc.lib.boxc.migration.cdm.services.sips.WorkGenerator.STREAMING_TYPE;
import static edu.unc.lib.boxc.migration.cdm.services.sips.WorkGenerator.STREAMING_URL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test which runs a single collection through a full set of migration steps
 * @author bbpennel
 */
@WireMockTest(httpPort = CdmEnvironmentHelper.TEST_HTTP_PORT)
public class CompleteMigrationIT extends AbstractCommandIT {
    private final static String COLLECTION_ID = "mini_gilmer";
    private final static String GROUPS = "my:admin:group";
    private final static String DEST_UUID = "3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e";
    private final static int REDIS_PORT = 46380;

    private TestSshServer testSshServer;
    private Path filesBasePath;

    private DepositStatusFactory depositStatusFactory;
    private JedisPool jedisPool;

    @BeforeEach
    public void setup() throws Exception {
        filesBasePath = tmpFolder;

        System.setProperty("REDIS_HOST", "localhost");
        System.setProperty("REDIS_PORT", Integer.toString(REDIS_PORT));

        testSshServer = new TestSshServer();
        testSshServer.startServer();

        setupChompbConfig();
    }

    private void mockFieldInfoUrl(String responseFile, String collectionId) throws IOException {
        String validRespBody = IOUtils.toString(this.getClass().getResourceAsStream("/" + responseFile),
                StandardCharsets.UTF_8);

        stubFor(get(urlEqualTo("/" + CdmFieldService.getFieldInfoUrl(collectionId)))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "text/json")
                        .withBody(validRespBody)));
    }

    public void initDepositStatusFactory() {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxIdle(15);
        jedisPoolConfig.setMaxTotal(25);
        jedisPoolConfig.setMinIdle(2);

        jedisPool = new JedisPool(jedisPoolConfig, "localhost", REDIS_PORT);
        depositStatusFactory = new DepositStatusFactory();
        depositStatusFactory.setJedisPool(jedisPool);
    }

    @AfterEach
    public void after() throws Exception {
        System.clearProperty("REDIS_HOST");
        System.clearProperty("REDIS_PORT");
        if (jedisPool != null) {
            jedisPool.close();
        }
        testSshServer.stopServer();
    }

    @Test
    public void migrateSimpleCollectionTest() throws Exception {
        mockFieldInfoUrl("cdm_fields_resp.json", COLLECTION_ID);

        String[] argsInit = new String[] {
                "-w", baseDir.toString(),
                "--env-config", chompbConfigPath,
                "init",
                "-p", COLLECTION_ID,
                "-e", "test"};
        executeExpectSuccess(argsInit);

        Path projPath = baseDir.resolve(COLLECTION_ID);
        MigrationProject project = new MigrationProject(projPath);

        String[] argsExport = new String[] {
                "-w", projPath.toString(),
                "--env-config", chompbConfigPath,
                "export",
                "-p", TestSshServer.PASSWORD };
        executeExpectSuccess(argsExport);

        String[] argsIndex = new String[] {
                "-w", projPath.toString(),
                "index"};
        executeExpectSuccess(argsIndex);

        String[] argsDest = new String[] {
                "-w",  projPath.toString(),
                "destinations", "generate",
                "-dd", DEST_UUID};
        executeExpectSuccess(argsDest);

        testHelper = new SipServiceHelper(project, filesBasePath);
        Path sourcePath1 = testHelper.addSourceFile("276_182_E.tif");
        Path sourcePath2 = testHelper.addSourceFile("276_183_E.tif");
        Path sourcePath3 = testHelper.addSourceFile("276_203_E.tif");

        String[] argsSource = new String[] {
                "-w", projPath.toString(),
                "source_files", "generate",
                "-b", testHelper.getSourceFilesBasePath().toString(),
                "-n", "file"};
        executeExpectSuccess(argsSource);

        Path accessPath1 = testHelper.addAccessFile("276_182_E.tif");
        String[] argsAccess = new String[] {
                "-w", projPath.toString(),
                "access_files", "generate",
                "-b", testHelper.getAccessFilesBasePath().toString(),
                "-n", "file"};
        executeExpectSuccess(argsAccess);

        Path altTextPath1 = testHelper.addAltTextFile("25.txt");
        String[] argsAltText = new String[] {
                "-w", projPath.toString(),
                "alt_text_files", "generate",
                "-b", testHelper.getAltTextFilesBasePath().toString()};
        executeExpectSuccess(argsAltText);

        Files.copy(Paths.get("src/test/resources/mods_collections/gilmer_mods1.xml"),
                project.getDescriptionsPath().resolve("gilmer_mods1.xml"));
        String[] argsDesc = new String[] {
                "-w", projPath.toString(),
                "descriptions", "expand" };
        executeExpectSuccess(argsDesc);

        String[] args = new String[] {
                "-w", projPath.toString(),
                "sips", "generate" };
        executeExpectSuccess(args);

        MigrationSip sip = testHelper.extractSipFromOutput(output);

        DepositDirectoryManager dirManager = testHelper.createDepositDirectoryManager(sip);
        Model model = testHelper.getSipModel(sip);

        Bag depBag = model.getBag(sip.getDepositPid().getRepositoryPath());
        List<RDFNode> depBagChildren = depBag.iterator().toList();
        assertEquals(3, depBagChildren.size());

        Resource workResc1 = testHelper.getResourceByCreateTime(depBagChildren, "2005-11-23");
        testHelper.assertObjectPopulatedInSip(workResc1, dirManager, model, sourcePath1, accessPath1, altTextPath1, "25");
        Resource workResc2 = testHelper.getResourceByCreateTime(depBagChildren, "2005-11-24");
        testHelper.assertObjectPopulatedInSip(workResc2, dirManager, model, sourcePath2, null, null, "26");
        Resource workResc3 = testHelper.getResourceByCreateTime(depBagChildren, "2005-12-08");
        testHelper.assertObjectPopulatedInSip(workResc3, dirManager, model, sourcePath3, null, null, "27");

        String[] argsSubmit = new String[] {
                "-w", projPath.toString(),
                "submit",
                "-g", GROUPS };
        executeExpectSuccess(argsSubmit);

        initDepositStatusFactory();
        assertDepositStatusSet(sip);
    }

    @Test
    public void migrateGroupedCollectionTest() throws Exception {
        mockFieldInfoUrl("cdm_fields_resp.json", "grouped_gilmer");

        String[] argsInit = new String[] {
                "-w", baseDir.toString(),
                "--env-config", chompbConfigPath,
                "init",
                "-p", "grouped_gilmer",
                "-e", "test"};
        executeExpectSuccess(argsInit);

        Path projPath = baseDir.resolve("grouped_gilmer");
        MigrationProject project = new MigrationProject(projPath);

        String[] argsExport = new String[] {
                "-w", projPath.toString(),
                "--env-config", chompbConfigPath,
                "export",
                "-p", TestSshServer.PASSWORD };
        executeExpectSuccess(argsExport);

        String[] argsIndex = new String[] {
                "-w", projPath.toString(),
                "index"};
        executeExpectSuccess(argsIndex);

        String[] argsDest = new String[] {
                "-w",  projPath.toString(),
                "destinations", "generate",
                "-dd", DEST_UUID};
        executeExpectSuccess(argsDest);

        String[] argsGroupGenerate = new String[] {
                "-w", project.getProjectPath().toString(),
                "group_mapping", "generate",
                "-n", "groupa"};
        executeExpectSuccess(argsGroupGenerate);

        String[] argsGroupSync = new String[] {
                "-w", project.getProjectPath().toString(),
                "group_mapping", "sync"};
        executeExpectSuccess(argsGroupSync);

        testHelper = new SipServiceHelper(project, filesBasePath);
        Path sourcePath1 = testHelper.addSourceFile("276_185_E.tif");
        Path sourcePath2 = testHelper.addSourceFile("276_183_E.tif");
        Path sourcePath3 = testHelper.addSourceFile("276_203_E.tif");
        Path sourcePath4 = testHelper.addSourceFile("276_241_E.tif");
        Path sourcePath5 = testHelper.addSourceFile("276_245a_E.tif");
        Path aggrPathTop = testHelper.addSourceFile("group1.pdf");
        Path aggrPathBottom = testHelper.addSourceFile("group1_bottom.pdf");

        String[] argsSource = new String[] {
                "-w", projPath.toString(),
                "source_files", "generate",
                "-b", testHelper.getSourceFilesBasePath().toString(),
                "-n", "file"};
        executeExpectSuccess(argsSource);

        Path accessPath1 = testHelper.addAccessFile("276_185_E.tif");
        String[] argsAccess = new String[] {
                "-w", projPath.toString(),
                "access_files", "generate",
                "-b", testHelper.getAccessFilesBasePath().toString(),
                "-n", "file"};
        executeExpectSuccess(argsAccess);

        String[] argsAggregateTop = new String[] {
                "-w", project.getProjectPath().toString(),
                "aggregate_files", "generate",
                "-b", testHelper.getSourceFilesBasePath().toString(),
                "-p", "(.+)",
                "-t", "$1.pdf",
                "-n", "groupa" };
        executeExpectSuccess(argsAggregateTop);

        String[] argsAggregateBottom = new String[] {
                "-w", project.getProjectPath().toString(),
                "aggregate_files", "generate",
                "-b", testHelper.getSourceFilesBasePath().toString(),
                "-p", "(.+)",
                "--sort-bottom",
                "-t", "$1_bottom.pdf",
                "-n", "groupa" };
        executeExpectSuccess(argsAggregateBottom);

        Files.copy(Paths.get("src/test/resources/mods_collections/grouped_mods.xml"),
                project.getDescriptionsPath().resolve("grouped_mods.xml"));
        String[] argsDesc = new String[] {
                "-w", projPath.toString(),
                "descriptions", "expand" };
        executeExpectSuccess(argsDesc);

        String[] args = new String[] {
                "-w", projPath.toString(),
                "sips", "generate" };
        executeExpectSuccess(args);

        MigrationSip sip = testHelper.extractSipFromOutput(output);

        DepositDirectoryManager dirManager = testHelper.createDepositDirectoryManager(sip);
        Model model = testHelper.getSipModel(sip);

        Bag depBag = model.getBag(sip.getDepositPid().getRepositoryPath());
        List<RDFNode> depBagChildren = depBag.iterator().toList();
        assertEquals(4, depBagChildren.size());

        Resource workResc1 = testHelper.getResourceByCreateTime(depBagChildren, "2005-11-23");
        Bag work1Bag = model.getBag(workResc1);
        testHelper.assertGroupedWorkPopulatedInSip(workResc1, dirManager, model, "grp:groupa:group1", true,
                aggrPathTop, null, sourcePath1, accessPath1, sourcePath2, null, aggrPathBottom, null);
        var work1OrderList = Arrays.asList(findFileIdByStagingLocation(work1Bag, aggrPathTop),
                findFileIdByStagingLocation(work1Bag, sourcePath2),
                findFileIdByStagingLocation(work1Bag, sourcePath1),
                findFileIdByStagingLocation(work1Bag, aggrPathBottom));
        var work1Members = String.join("|", work1OrderList);
        assertTrue(workResc1.hasProperty(Cdr.memberOrder, work1Members));
        Resource workResc2 = testHelper.getResourceByCreateTime(depBagChildren, "2005-12-08");
        testHelper.assertObjectPopulatedInSip(workResc2, dirManager, model, sourcePath3, null, null, "27");
        Resource workResc3 = testHelper.getResourceByCreateTime(depBagChildren, "2005-12-09");
        testHelper.assertObjectPopulatedInSip(workResc3, dirManager, model, sourcePath4, null, null, "28");
        Resource workResc4 = testHelper.getResourceByCreateTime(depBagChildren, "2005-12-10");
        testHelper.assertObjectPopulatedInSip(workResc4, dirManager, model, sourcePath5, null, null, "29");

        String[] argsSubmit = new String[] {
                "-w", projPath.toString(),
                "submit",
                "-g", GROUPS };
        executeExpectSuccess(argsSubmit);

        initDepositStatusFactory();
        assertDepositStatusSet(sip);
    }

    @Test
    public void fileWithStreamingOnlyNoSourceFileTest() throws Exception {
        mockFieldInfoUrl("cdm_fields_resp.json", COLLECTION_ID);

        String[] argsInit = new String[] {
                "-w", baseDir.toString(),
                "--env-config", chompbConfigPath,
                "init",
                "-p", COLLECTION_ID,
                "-e", "test"};
        executeExpectSuccess(argsInit);

        Path projPath = baseDir.resolve(COLLECTION_ID);
        MigrationProject project = new MigrationProject(projPath);

        String[] argsExport = new String[] {
                "-w", projPath.toString(),
                "--env-config", chompbConfigPath,
                "export",
                "-p", TestSshServer.PASSWORD };
        executeExpectSuccess(argsExport);

        String[] argsIndex = new String[] {
                "-w", projPath.toString(),
                "index"};
        executeExpectSuccess(argsIndex);

        String[] argsDest = new String[] {
                "-w",  projPath.toString(),
                "destinations", "generate",
                "-dd", DEST_UUID};
        executeExpectSuccess(argsDest);

        testHelper = new SipServiceHelper(project, filesBasePath);
        Path sourcePath1 = testHelper.addSourceFile("276_182_E.tif");
        Path sourcePath2 = testHelper.addSourceFile("276_183_E.tif");
        // No source file item 27, so that it will only have streaming

        String[] argsSource = new String[] {
                "-w", projPath.toString(),
                "source_files", "generate",
                "-b", testHelper.getSourceFilesBasePath().toString(),
                "-n", "file"};
        executeExpectSuccess(argsSource);

        Files.copy(Paths.get("src/test/resources/mods_collections/gilmer_mods1.xml"),
                project.getDescriptionsPath().resolve("gilmer_mods1.xml"));
        String[] argsDesc = new String[] {
                "-w", projPath.toString(),
                "descriptions", "expand" };
        executeExpectSuccess(argsDesc);

        String[] args = new String[] {
                "-w", projPath.toString(),
                "sips", "generate" };
        executeExpectSuccess(args);

        MigrationSip sip = testHelper.extractSipFromOutput(output);

        DepositDirectoryManager dirManager = testHelper.createDepositDirectoryManager(sip);
        Model model = testHelper.getSipModel(sip);

        Bag depBag = model.getBag(sip.getDepositPid().getRepositoryPath());
        List<RDFNode> depBagChildren = depBag.iterator().toList();
        assertEquals(3, depBagChildren.size());

        Resource workResc1 = testHelper.getResourceByCreateTime(depBagChildren, "2005-11-23");
        testHelper.assertObjectPopulatedInSip(workResc1, dirManager, model, sourcePath1, null, null, "25");
        // Work 2 has a source file and a streaming url
        Resource workResc2 = testHelper.getResourceByCreateTime(depBagChildren, "2005-11-24");
        testHelper.assertObjectPopulatedInSip(workResc2, dirManager, model, sourcePath2, null, null, "26");
        Resource fileResc2 = testHelper.getFirstSipFileInWork(workResc2, dirManager, model);
        assertTrue(fileResc2.hasProperty(STREAMING_URL));
        assertTrue(fileResc2.hasProperty(STREAMING_TYPE));
        // Work 3 has no source file, but does have a streaming url
        Resource workResc3 = testHelper.getResourceByCreateTime(depBagChildren, "2005-12-08");
        testHelper.assertObjectPopulatedInSip(workResc3, dirManager, model, null, null, null, "27");
        Resource fileResc3 = testHelper.getFirstSipFileInWork(workResc3, dirManager, model);
        assertTrue(fileResc3.hasProperty(STREAMING_URL));
        assertTrue(fileResc3.hasProperty(STREAMING_TYPE));

        String[] argsSubmit = new String[] {
                "-w", projPath.toString(),
                "submit",
                "-g", GROUPS };
        executeExpectSuccess(argsSubmit);

        initDepositStatusFactory();
        assertDepositStatusSet(sip);
    }

    private String findFileIdByStagingLocation(Bag workBag, Path stagingLoc) {
        Resource fileResc = testHelper.findChildByStagingLocation(workBag, stagingLoc);
        return PIDs.get(fileResc.getURI()).getId();
    }

    private void assertDepositStatusSet(MigrationSip sip) {
        Map<String, String> status = depositStatusFactory.get(sip.getDepositId());
        String sourceUri = status.get(DepositField.sourceUri.name());
        assertEquals(sip.getSipPath(), Paths.get(URI.create(sourceUri)));
        assertEquals(USERNAME + "@ad.unc.edu", status.get(DepositField.depositorEmail.name()));
        assertEquals("unc:onyen:theuser;my:admin:group", status.get(DepositField.permissionGroups.name()));
        assertEquals(PackagingType.BAG_WITH_N3.getUri(), status.get(DepositField.packagingType.name()));
    }
}
