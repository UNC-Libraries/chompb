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

import edu.unc.lib.boxc.deposit.api.RedisWorkerConstants.DepositField;
import edu.unc.lib.boxc.deposit.impl.model.DepositStatusFactory;
import edu.unc.lib.boxc.migration.cdm.model.MigrationSip;
import edu.unc.lib.boxc.migration.cdm.options.SipGenerationOptions;
import edu.unc.lib.boxc.migration.cdm.services.AccessFileService;
import edu.unc.lib.boxc.migration.cdm.services.CdmIndexService;
import edu.unc.lib.boxc.migration.cdm.services.DescriptionsService;
import edu.unc.lib.boxc.migration.cdm.services.DestinationsService;
import edu.unc.lib.boxc.migration.cdm.services.SipService;
import edu.unc.lib.boxc.migration.cdm.services.SourceFileService;
import edu.unc.lib.boxc.model.api.ids.PIDMinter;
import edu.unc.lib.boxc.model.fcrepo.ids.RepositoryPIDMinter;
import edu.unc.lib.boxc.operations.impl.events.PremisLoggerFactoryImpl;
import edu.unc.lib.boxc.persist.api.PackagingType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.embedded.RedisServer;

import java.io.BufferedWriter;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static java.nio.file.StandardOpenOption.APPEND;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author bbpennel
 */
public class SubmitSipsCommandIT extends AbstractCommandIT {
    private final static String DEST_UUID = "3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e";
    private static final String DEST_UUID2 = "8ae56bbc-400e-496d-af4b-3c585e20dba1";
    private final static int REDIS_PORT = 46380;
    private final static String GROUPS = "my:admin:group";

    private RedisServer redisServer;
    private SipService sipService;

    private DepositStatusFactory depositStatusFactory;
    private JedisPool jedisPool;

    private SourceFileService sourceFileService;
    private AccessFileService accessFileService;
    private DescriptionsService descriptionsService;
    private DestinationsService destinationsService;
    private CdmIndexService indexService;
    private PIDMinter pidMinter;
    private PremisLoggerFactoryImpl premisLoggerFactory;

    @Before
    public void setup() throws Exception {
        initProjectAndHelper();
        redisServer = new RedisServer(REDIS_PORT);
        System.setProperty("REDIS_HOST", "localhost");
        System.setProperty("REDIS_PORT", Integer.toString(REDIS_PORT));
        redisServer.start();
        sipService = new SipService();
        sipService.setProject(project);

        pidMinter = new RepositoryPIDMinter();
        premisLoggerFactory = new PremisLoggerFactoryImpl();
        premisLoggerFactory.setPidMinter(pidMinter);
        indexService = new CdmIndexService();
        indexService.setProject(project);
        sourceFileService = new SourceFileService();
        sourceFileService.setIndexService(indexService);
        sourceFileService.setProject(project);
        accessFileService = new AccessFileService();
        accessFileService.setIndexService(indexService);
        accessFileService.setProject(project);
        descriptionsService = new DescriptionsService();
        descriptionsService.setProject(project);
        destinationsService = new DestinationsService();
        destinationsService.setProject(project);

        sipService = new SipService();
        sipService.setIndexService(indexService);
        sipService.setAccessFileService(accessFileService);
        sipService.setSourceFileService(sourceFileService);
        sipService.setPidMinter(pidMinter);
        sipService.setDescriptionsService(descriptionsService);
        sipService.setPremisLoggerFactory(premisLoggerFactory);
        sipService.setProject(project);
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
        if (jedisPool != null) {
            jedisPool.close();
        }
        redisServer.stop();
    }

    @Test
    public void submitMissingGroupsTest() throws Exception {
        testHelper.initializeDefaultProjectState(DEST_UUID);
        SipGenerationOptions genOptions = new SipGenerationOptions();
        genOptions.setUsername(USERNAME);
        List<MigrationSip> sips = sipService.generateSips(genOptions);
        MigrationSip sip = sips.get(0);

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "submit" };
        executeExpectFailure(args);

        assertOutputContains("Must provide one or more groups");

        initDepositStatusFactory();
        assertTrue(depositStatusFactory.get(sip.getDepositId()).isEmpty());
    }

    @Test
    public void submitInvalidPortTest() throws Exception {
        System.setProperty("REDIS_PORT", "0");
        testHelper.initializeDefaultProjectState(DEST_UUID);
        SipGenerationOptions genOptions = new SipGenerationOptions();
        genOptions.setUsername(USERNAME);
        List<MigrationSip> sips = sipService.generateSips(genOptions);
        MigrationSip sip = sips.get(0);

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "submit",
                "-g", GROUPS};
        executeExpectFailure(args);

        assertOutputContains("Must provide a valid Redis port number");

        initDepositStatusFactory();
        assertTrue(depositStatusFactory.get(sip.getDepositId()).isEmpty());
    }

    @Test
    public void submitNoRedisHostTest() throws Exception {
        System.setProperty("REDIS_HOST", "");
        testHelper.initializeDefaultProjectState(DEST_UUID);
        SipGenerationOptions genOptions = new SipGenerationOptions();
        genOptions.setUsername(USERNAME);
        List<MigrationSip> sips = sipService.generateSips(genOptions);
        MigrationSip sip = sips.get(0);

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "submit",
                "-g", GROUPS};
        executeExpectFailure(args);

        assertOutputContains("Must a Redis host URI");

        initDepositStatusFactory();
        assertTrue(depositStatusFactory.get(sip.getDepositId()).isEmpty());
    }

    @Test
    public void submitSingleSipTest() throws Exception {
        testHelper.initializeDefaultProjectState(DEST_UUID);
        SipGenerationOptions genOptions = new SipGenerationOptions();
        genOptions.setUsername(USERNAME);
        List<MigrationSip> sips = sipService.generateSips(genOptions);
        MigrationSip sip = sips.get(0);

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "submit",
                "-g", GROUPS };
        executeExpectSuccess(args);

        assertOutputContains("Submitted SIP " + sip.getDepositId() + " for deposit to " + DEST_UUID);

        initDepositStatusFactory();
        assertDepositStatusSet(sip);
    }

    @Test
    public void submitMultipleSipTest() throws Exception {
        testHelper.initializeDefaultProjectState(DEST_UUID);
        // Inserting an extra destination which has one object mapped to it
        try (BufferedWriter writer = Files.newBufferedWriter(project.getDestinationMappingsPath(), APPEND)) {
            writer.write("26," + DEST_UUID2 + ",");
        }
        SipGenerationOptions genOptions = new SipGenerationOptions();
        genOptions.setUsername(USERNAME);
        List<MigrationSip> sips = sipService.generateSips(genOptions);
        MigrationSip sip1 = sips.get(0);
        MigrationSip sip2 = sips.get(1);

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "submit",
                "-g", GROUPS };
        executeExpectSuccess(args);

        assertOutputContains("Submitted SIP " + sip1.getDepositId() + " for deposit to " + DEST_UUID);
        assertOutputContains("Submitted SIP " + sip2.getDepositId() + " for deposit to " + DEST_UUID2);

        initDepositStatusFactory();
        assertDepositStatusSet(sip1);
        assertDepositStatusSet(sip2);
    }

    @Test
    public void submitExcludeAllTest() throws Exception {
        testHelper.initializeDefaultProjectState(DEST_UUID);
        SipGenerationOptions genOptions = new SipGenerationOptions();
        genOptions.setUsername(USERNAME);
        List<MigrationSip> sips = sipService.generateSips(genOptions);
        MigrationSip sip = sips.get(0);

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "submit",
                "-g", GROUPS,
                "-i", "notreal" };
        executeExpectSuccess(args);

        assertOutputContains("Skipping SIP " + sip.getDepositId());

        initDepositStatusFactory();
        assertTrue(depositStatusFactory.get(sip.getDepositId()).isEmpty());
    }

    @Test
    public void submitMultipleLimitToOneTest() throws Exception {
        testHelper.initializeDefaultProjectState(DEST_UUID);
        // Inserting an extra destination which has one object mapped to it
        try (BufferedWriter writer = Files.newBufferedWriter(project.getDestinationMappingsPath(), APPEND)) {
            writer.write("26," + DEST_UUID2 + ",");
        }
        SipGenerationOptions genOptions = new SipGenerationOptions();
        genOptions.setUsername(USERNAME);
        List<MigrationSip> sips = sipService.generateSips(genOptions);
        MigrationSip sip1 = sips.get(0);
        MigrationSip sip2 = sips.get(1);

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "submit",
                "-g", GROUPS,
                "-i", sip2.getDepositId()};
        executeExpectSuccess(args);

        assertOutputDoesNotContain("Submitted SIP " + sip1.getDepositId());
        assertOutputContains("Skipping SIP " + sip1.getDepositId());
        assertOutputContains("Submitted SIP " + sip2.getDepositId() + " for deposit to " + DEST_UUID2);

        initDepositStatusFactory();
        assertDepositStatusSet(sip2);
        assertTrue(depositStatusFactory.get(sip1.getDepositId()).isEmpty());
    }

    private void assertDepositStatusSet(MigrationSip sip) {
        Map<String, String> status = depositStatusFactory.get(sip.getDepositId());
        String sourceUri = status.get(DepositField.sourceUri.name());
        assertEquals(sip.getSipPath(), Paths.get(URI.create(sourceUri)));
        assertEquals("true", status.get(DepositField.excludeDepositRecord.name()));
        assertEquals(USERNAME + "@ad.unc.edu", status.get(DepositField.depositorEmail.name()));
        assertEquals("Cdm2Bxc my_proj " + sip.getDepositId(), status.get(DepositField.depositSlug.name()));
        assertEquals("unc:onyen:theuser;my:admin:group", status.get(DepositField.permissionGroups.name()));
        assertEquals(PackagingType.BAG_WITH_N3.getUri(), status.get(DepositField.packagingType.name()));
    }
}
