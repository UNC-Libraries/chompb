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

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.jena.rdf.model.Bag;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.github.tomakehurst.wiremock.junit.WireMockRule;

import edu.unc.lib.boxc.deposit.api.RedisWorkerConstants.DepositField;
import edu.unc.lib.boxc.deposit.impl.model.DepositDirectoryManager;
import edu.unc.lib.boxc.deposit.impl.model.DepositStatusFactory;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.MigrationSip;
import edu.unc.lib.boxc.migration.cdm.services.CdmFieldService;
import edu.unc.lib.boxc.migration.cdm.test.SipServiceHelper;
import edu.unc.lib.boxc.persist.api.PackagingType;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.embedded.RedisServer;

/**
 * Test which runs a single collection through a full set of migration steps
 * @author bbpennel
 */
public class CompleteMigrationIT extends AbstractCommandIT {
    private final static String COLLECTION_ID = "my_coll";
    private final static String CDM_PASSWORD = "supersecret";
    private final static String GROUPS = "my:admin:group";
    private final static String DEST_UUID = "3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e";
    private final static int REDIS_PORT = 46380;

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(options().dynamicPort());
    private String cdmBaseUrl;
    private Path filesBasePath;

    private RedisServer redisServer;
    private DepositStatusFactory depositStatusFactory;
    private JedisPool jedisPool;

    @Before
    public void setup() throws Exception {
        filesBasePath = tmpFolder.newFolder().toPath();
        cdmBaseUrl = "http://localhost:" + wireMockRule.port();
        String validRespBody = IOUtils.toString(this.getClass().getResourceAsStream("/cdm_fields_resp.json"),
                StandardCharsets.UTF_8);

        stubFor(get(urlEqualTo("/" + CdmFieldService.getFieldInfoUrl(COLLECTION_ID)))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "text/json")
                        .withBody(validRespBody)));
        stubFor(post(urlEqualTo("/cgi-bin/admin/exportxml.exe"))
                .willReturn(aResponse()
                        .withStatus(200)));
        String exportBody = FileUtils.readFileToString(new File("src/test/resources/sample_exports/export_1.xml"),
                StandardCharsets.ISO_8859_1);
        stubFor(get(urlEqualTo("/cgi-bin/admin/getfile.exe?CISOMODE=1&CISOFILE=/my_coll/index/description/export.xml"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/octet-stream")
                        .withBody(exportBody)));

        redisServer = new RedisServer(REDIS_PORT);
        System.setProperty("REDIS_HOST", "localhost");
        System.setProperty("REDIS_PORT", Integer.toString(REDIS_PORT));
        redisServer.start();
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

    @After
    public void after() throws Exception {
        System.clearProperty("REDIS_HOST");
        System.clearProperty("REDIS_PORT");
        redisServer.stop();
        if (jedisPool != null) {
            jedisPool.close();
        }
    }

    @Test
    public void migrateSimpleCollectionTest() throws Exception {
        stubFor(get(urlEqualTo("/dmwebservices/index.php?q=dmQuery/my_coll/0/dmrecord/dmrecord/1/0/1/0/0/0/0/json"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/octet-stream")
                        .withBody(IOUtils.toString(getClass().getResourceAsStream("/sample_pages/cdm_listid_resp.json"), StandardCharsets.UTF_8))));
        stubFor(get(urlEqualTo("/dmwebservices/index.php?q=dmQuery/my_coll/0/dmrecord/dmrecord/1000/1/1/0/0/0/0/json"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/octet-stream")
                        .withBody(IOUtils.toString(getClass().getResourceAsStream("/sample_pages/page_all.json"), StandardCharsets.UTF_8))));

        String[] argsInit = new String[] {
                "-w", baseDir.toString(),
                "init",
                "--cdm-url", cdmBaseUrl,
                "-p", COLLECTION_ID };
        executeExpectSuccess(argsInit);

        Path projPath = baseDir.resolve(COLLECTION_ID);
        MigrationProject project = new MigrationProject(projPath);

        String[] argsExport = new String[] {
                "-w", projPath.toString(),
                "export",
                "--cdm-url", cdmBaseUrl,
                "-p", CDM_PASSWORD };
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

        SipServiceHelper testHelper = new SipServiceHelper(project, filesBasePath);
        Path sourcePath1 = testHelper.addSourceFile("276_182_E.tif");
        Path sourcePath2 = testHelper.addSourceFile("276_183B_E.tif");
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
        testHelper.assertObjectPopulatedInSip(workResc1, dirManager, model, sourcePath1, accessPath1, "25");
        Resource workResc2 = testHelper.getResourceByCreateTime(depBagChildren, "2005-11-24");
        testHelper.assertObjectPopulatedInSip(workResc2, dirManager, model, sourcePath2, null, "26");
        Resource workResc3 = testHelper.getResourceByCreateTime(depBagChildren, "2005-12-08");
        testHelper.assertObjectPopulatedInSip(workResc3, dirManager, model, sourcePath3, null, "27");

        String[] argsSubmit = new String[] {
                "-w", projPath.toString(),
                "submit",
                "-g", GROUPS };
        executeExpectSuccess(argsSubmit);

        initDepositStatusFactory();
        assertDepositStatusSet(sip);
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
