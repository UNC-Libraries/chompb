package edu.unc.lib.boxc.migration.cdm.services;

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.util.Set;

import org.slf4j.Logger;

import edu.unc.lib.boxc.auth.api.models.AgentPrincipals;
import edu.unc.lib.boxc.auth.fcrepo.models.AccessGroupSetImpl;
import edu.unc.lib.boxc.auth.fcrepo.models.AgentPrincipalsImpl;
import edu.unc.lib.boxc.deposit.api.DepositMethod;
import edu.unc.lib.boxc.deposit.api.RedisWorkerConstants.Priority;
import edu.unc.lib.boxc.deposit.api.exceptions.DepositException;
import edu.unc.lib.boxc.deposit.api.submit.DepositData;
import edu.unc.lib.boxc.deposit.impl.model.DepositStatusFactory;
import edu.unc.lib.boxc.deposit.impl.submit.PreconstructedDepositHandler;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.MigrationSip;
import edu.unc.lib.boxc.migration.cdm.options.SipSubmissionOptions;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import edu.unc.lib.boxc.persist.api.PackagingType;

/**
 * Service which submits SIPs for deposit
 *
 * @author bbpennel
 */
public class SipSubmissionService {
    private static final Logger log = getLogger(SipSubmissionService.class);
    private static final String EMAIL_SUFFIX = "@ad.unc.edu";

    private DepositStatusFactory depositStatusFactory;
    private SipService sipService;
    private MigrationProject project;

    public SipSubmissionService() {
    }

    /**
     * Submit the SIPs generated for this project for deposit
     * @param options
     */
    public void submitSipsForDeposit(SipSubmissionOptions options) {
        AgentPrincipals principals = new AgentPrincipalsImpl(options.getUsername(),
                new AccessGroupSetImpl(options.getGroups()));

        Set<String> previouslySubmitted = project.getProjectProperties().getSipsSubmitted();
        for (MigrationSip sip : sipService.listSips()) {
            if (options.getSipIds() != null && !options.getSipIds().contains(sip.getDepositId())) {
                outputLogger.info("Skipping SIP {}", sip.getDepositId());
                continue;
            }
            if (options.isForce() || !previouslySubmitted.contains(sip.getDepositId())) {
                submitSip(sip, principals);

                project.getProjectProperties().getSipsSubmitted().add(sip.getDepositId());
                try {
                    ProjectPropertiesSerialization.write(project);
                } catch (IOException e) {
                    throw new MigrationException("Failed to update properties", e);
                }
            }
        }
    }

    private void submitSip(MigrationSip sip, AgentPrincipals principals) {
        log.info("Submitting sip {}", sip.getDepositId());
        DepositData depositData = new DepositData(null, null, PackagingType.BAG_WITH_N3,
                DepositMethod.CDM_TO_BXC_MIGRATION.getLabel(), principals);
        depositData.setDepositorEmail(principals.getUsername() + EMAIL_SUFFIX);
        depositData.setOverrideTimestamps(true);
        depositData.setPriority(Priority.low);
        depositData.setSourceUri(sip.getSipPath().toUri());
        String label = sip.getNewCollectionLabel() == null ? sip.getDepositId() : sip.getNewCollectionLabel();
        depositData.setSlug("Cdm2Bxc " + project.getProjectName() + " " + label);
        log.debug("Compiled deposit data for sip {}", sip.getDepositId());

        PreconstructedDepositHandler depositHandler = new PreconstructedDepositHandler(sip.getDepositPid());
        depositHandler.setDepositStatusFactory(depositStatusFactory);
        log.debug("Initialized deposit handler for sip {}", sip.getDepositId());
        try {
            depositHandler.doDeposit(sip.getDestinationPid(), depositData);
            outputLogger.info("Submitted SIP {} for deposit to {}", sip.getDepositId(), sip.getDestinationId());
        } catch (DepositException e) {
            throw new MigrationException("Failed to submit deposit", e);
        }
    }

    public void setDepositStatusFactory(DepositStatusFactory depositStatusFactory) {
        this.depositStatusFactory = depositStatusFactory;
    }

    public void setSipService(SipService sipService) {
        this.sipService = sipService;
    }

    public void setProject(MigrationProject project) {
        this.project = project;
    }
}
