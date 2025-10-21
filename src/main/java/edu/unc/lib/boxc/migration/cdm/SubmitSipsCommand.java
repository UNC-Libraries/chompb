package edu.unc.lib.boxc.migration.cdm;

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.Callable;

import edu.unc.lib.boxc.deposit.impl.jms.DepositOperationMessageService;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import edu.unc.lib.boxc.deposit.impl.model.DepositStatusFactory;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.SipSubmissionOptions;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.services.SipService;
import edu.unc.lib.boxc.migration.cdm.services.SipSubmissionService;
import org.springframework.jms.core.JmsTemplate;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.ParentCommand;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @author bbpennel
 */
@Command(name = "submit",
        description = "Submit generated SIPs for deposit")
public class SubmitSipsCommand implements Callable<Integer> {
    private static final Logger log = getLogger(SubmitSipsCommand.class);
    @ParentCommand
    private CLIMain parentCommand;

    private SipService sipService;
    private MigrationProject project;
    private SipSubmissionService submissionService;
    private JedisPool jedisPool;

    @Mixin
    private SipSubmissionOptions options;

    @Override
    public Integer call() throws Exception {
        long start = System.nanoTime();

        try {
            validateOptions(options);
            initialize(options);

            submissionService.submitSipsForDeposit(options);

            outputLogger.info("Completed operation for project {} in {}s", project.getProjectName(),
                    (System.nanoTime() - start) / 1e9);
            return 0;
        } catch (MigrationException | IllegalArgumentException e) {
            outputLogger.info("Cannot submit SIPs: {}", e.getMessage());
            return 1;
        } catch (Exception e) {
            log.error("Failed to submit SIPs", e);
            outputLogger.info("Failed to submit SIPs: {}", e.getMessage(), e);
            return 1;
        } finally {
            if (jedisPool != null) {
                jedisPool.close();
            }
        }
    }

    private void validateOptions(SipSubmissionOptions options) {
        if (StringUtils.isBlank(options.getGroups())) {
            throw new IllegalArgumentException("Must provide one or more groups");
        }
        if (StringUtils.isBlank(options.getRedisHost())) {
            throw new IllegalArgumentException("Must a Redis host URI");
        }
        if (options.getRedisPort() <= 0) {
            throw new IllegalArgumentException("Must provide a valid Redis port number");
        }
    }

    private void initialize(SipSubmissionOptions options) throws IOException {
        Path currentPath = parentCommand.getWorkingDirectory();
        project = MigrationProjectFactory.loadMigrationProject(currentPath);

        sipService = new SipService();
        sipService.setProject(project);

        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxIdle(15);
        jedisPoolConfig.setMaxTotal(25);
        jedisPoolConfig.setMinIdle(2);

        jedisPool = new JedisPool(jedisPoolConfig, options.getRedisHost(), options.getRedisPort());
        DepositStatusFactory depositStatusFactory = new DepositStatusFactory();
        depositStatusFactory.setJedisPool(jedisPool);

        DepositOperationMessageService depositOperationMessageService = getDepositOperationMessageService(options);

        submissionService = new SipSubmissionService();
        submissionService.setProject(project);
        submissionService.setSipService(sipService);
        submissionService.setDepositStatusFactory(depositStatusFactory);
        submissionService.setDepositOperationMessageService(depositOperationMessageService);
    }

    private static DepositOperationMessageService getDepositOperationMessageService(SipSubmissionOptions options) {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(options.getBrokerUrl());
        JmsTemplate jmsTemplate = new JmsTemplate();
        jmsTemplate.setConnectionFactory(connectionFactory);
        jmsTemplate.setDeliveryPersistent(true);
        jmsTemplate.setPubSubDomain(false);

        DepositOperationMessageService depositOperationMessageService = new DepositOperationMessageService();
        depositOperationMessageService.setJmsTemplate(jmsTemplate);
        depositOperationMessageService.setDestinationName(options.getJmsEndpoint());
        return depositOperationMessageService;
    }
}
