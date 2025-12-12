package edu.unc.lib.boxc.migration.cdm;

import edu.unc.lib.boxc.deposit.api.RedisWorkerConstants.DepositField;
import edu.unc.lib.boxc.deposit.impl.jms.DepositOperationMessage;
import edu.unc.lib.boxc.deposit.impl.jms.DepositOperationMessageService;
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
import edu.unc.lib.boxc.operations.impl.events.PremisLoggerFactoryImpl;
import edu.unc.lib.boxc.persist.api.PackagingType;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.BufferedWriter;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static java.nio.file.StandardOpenOption.APPEND;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author bbpennel
 */
public class SubmitSipsCommandIT extends AbstractCommandIT {
    private final static String DEST_UUID = "3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e";
    private static final String DEST_UUID2 = "8ae56bbc-400e-496d-af4b-3c585e20dba1";
    private final static String GROUPS = "my:admin:group";

    private SipService sipService;

    private ConnectionFactory connectionFactory;
    private Connection jmsConnection;
    private Session jmsSession;
    private MessageConsumer messageConsumer;

    @BeforeEach
    public void setup() throws Exception {
        initProjectAndHelper();
        System.setProperty("BROKER_URL", "tcp://localhost:46161");
        sipService = testHelper.createSipsService();
        setupJmsConsumer();
    }

    @AfterEach
    public void tearDown() throws Exception {
        System.clearProperty("BROKER_URL");

        if (messageConsumer != null) {
            while (messageConsumer.receiveNoWait() != null) {
                // Drain any extra messages
            }
            messageConsumer.close();
        }
        if (jmsSession != null) {
            jmsSession.close();
        }
        if (jmsConnection != null) {
            jmsConnection.close();
        }
    }

    private void setupJmsConsumer() throws Exception {
        if (connectionFactory == null) {
            connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:46161");
        }
        if (jmsConnection == null || !((org.apache.activemq.ActiveMQConnection) jmsConnection).isStarted()) {
            jmsConnection = connectionFactory.createConnection();
            jmsSession = jmsConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = jmsSession.createQueue("activemq:queue:deposit.operation.queue");
            messageConsumer = jmsSession.createConsumer(queue);
            jmsConnection.start();
            while (messageConsumer.receiveNoWait() != null) {
                // Drain any extra messages
            }
        }
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

        assertDepositNotSubmitted();
    }

    @Test
    public void submitNoBrokerTest() throws Exception {
        System.setProperty("BROKER_URL", "");
        testHelper.initializeDefaultProjectState(DEST_UUID);
        SipGenerationOptions genOptions = new SipGenerationOptions();
        genOptions.setUsername(USERNAME);
        List<MigrationSip> sips = sipService.generateSips(genOptions);
        MigrationSip sip = sips.getFirst();

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "submit",
                "-g", GROUPS};
        executeExpectFailure(args);

        assertOutputContains("Must provide a broker URL");

        assertDepositNotSubmitted();
    }

    @Test
    public void submitNoJmsEndpointTest() throws Exception {
        System.setProperty("JMS_ENDPOINT", "");
        testHelper.initializeDefaultProjectState(DEST_UUID);
        SipGenerationOptions genOptions = new SipGenerationOptions();
        genOptions.setUsername(USERNAME);
        List<MigrationSip> sips = sipService.generateSips(genOptions);
        MigrationSip sip = sips.getFirst();

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "submit",
                "-g", GROUPS};
        executeExpectFailure(args);

        assertOutputContains("Must provide a JMS endpoint name");

        assertDepositNotSubmitted();
    }

    @Test
    public void submitSingleSipTest() throws Exception {
        testHelper.initializeDefaultProjectState(DEST_UUID);
        SipGenerationOptions genOptions = new SipGenerationOptions();
        genOptions.setUsername(USERNAME);
        List<MigrationSip> sips = sipService.generateSips(genOptions);
        MigrationSip sip = sips.getFirst();

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "submit",
                "-g", GROUPS };
        executeExpectSuccess(args);

        assertOutputContains("Submitted SIP " + sip.getDepositId() + " for deposit to " + DEST_UUID);

        assertDepositStatusSet(sip, receiveDepositMessage());
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

        // Message order is not guaranteed, so we need to check deposit IDs
        var op1 = receiveDepositMessage();
        var op2 = receiveDepositMessage();
        if (op1.getDepositId().equals(sip1.getDepositId())) {
            assertDepositStatusSet(sip1, op1);
            assertDepositStatusSet(sip2, op2);
        } else {
            assertDepositStatusSet(sip1, op2);
            assertDepositStatusSet(sip2, op1);
        }
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

        assertDepositNotSubmitted();
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

        assertDepositStatusSet(sip2, receiveDepositMessage());
    }

    private void assertDepositNotSubmitted() throws Exception {
        // Receive message from queue with 2 second timeout
        Message message = messageConsumer.receive(200);
        assertNull(message, "Did not expect a deposit message for SIP");
    }

    private DepositOperationMessage receiveDepositMessage() throws Exception {
        // Receive message from queue with 5 second timeout
        Message message = messageConsumer.receive(5000);
        // Deserialize the message
        return DepositOperationMessageService.fromJson(message);
    }

    private void assertDepositStatusSet(MigrationSip sip, DepositOperationMessage depositMsg) throws Exception {
        Map<String, String> status = depositMsg.getAdditionalInfo();

        String sourceUri = status.get(DepositField.sourceUri.name());
        assertEquals(sip.getSipPath(), Paths.get(URI.create(sourceUri)));
        assertEquals("true", status.get(DepositField.excludeDepositRecord.name()));
        assertEquals(USERNAME + "@ad.unc.edu", status.get(DepositField.depositorEmail.name()));
        assertEquals("Cdm2Bxc my_proj " + sip.getDepositId(), status.get(DepositField.depositSlug.name()));
        assertEquals("unc:onyen:theuser;my:admin:group", status.get(DepositField.permissionGroups.name()));
        assertEquals(PackagingType.BAG_WITH_N3.getUri(), status.get(DepositField.packagingType.name()));
    }
}
