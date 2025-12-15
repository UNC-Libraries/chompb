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
        }
    }

    private void validateOptions(SipSubmissionOptions options) {
        if (StringUtils.isBlank(options.getGroups())) {
            throw new IllegalArgumentException("Must provide one or more groups");
        }
        if (StringUtils.isBlank(options.getBrokerUrl())) {
            throw new IllegalArgumentException("Must provide a broker URL");
        }
        if (StringUtils.isBlank(options.getJmsEndpoint())) {
            throw new IllegalArgumentException("Must provide a JMS endpoint name");
        }
    }

    private void initialize(SipSubmissionOptions options) throws IOException {
        Path currentPath = parentCommand.getWorkingDirectory();
        project = MigrationProjectFactory.loadMigrationProject(currentPath);

        sipService = new SipService();
        sipService.setProject(project);

        DepositOperationMessageService depositOperationMessageService = getDepositOperationMessageService(options);

        submissionService = new SipSubmissionService();
        submissionService.setProject(project);
        submissionService.setSipService(sipService);
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
