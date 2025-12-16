package edu.unc.lib.boxc.migration.cdm.options;

import java.util.List;

import picocli.CommandLine.Option;

/**
 * Options when submitting SIPs for deposit
 *
 * @author bbpennel
 */
public class SipSubmissionOptions {

    @Option(names = { "-i", "--sip-ids"},
            split = ",",
            description = {"UUIDs of SIPs to submit for deposit, comma separated.",
                    "If not provided, then all SIPs will be submitted"})
    private List<String> sipIds;

    @Option(names = { "-u", "--user"},
            description = {"Username of user performing the deposit.",
                    "Defaults to current user: ${DEFAULT-VALUE}"},
            defaultValue = "${sys:user.name}")
    private String username;

    @Option(names = { "-g", "--groups"},
            description = { "Authorization groups to supply with the deposit, semicolon separated.",
                    "Defaults to value of BOXC_GROUPS env variable: ${DEFAULT-VALUE}" },
            defaultValue = "${env:BOXC_GROUPS}" )
    private String groups;

    @Option(names = { "--broker-url"},
            description = { "URL of the JMS broker to use for submitting to the deposit pipeline.",
                    "Defaults to using the BROKER_URL variable: ${DEFAULT-VALUE}" },
            defaultValue = "${env:BROKER_URL:-${sys:BROKER_URL:-tcp://localhost:61616}}")
    private String brokerUrl;

    @Option(names = { "--jms-endpoint"},
            description = { "Name of the jms endpoint to send messages to.",
                    "Defaults to using the JMS_ENDPOINT variable: ${DEFAULT-VALUE}" },
            defaultValue = "${env:JMS_ENDPOINT:-${sys:JMS_ENDPOINT:-activemq:queue:deposit.operation.queue}}")
    private String jmsEndpoint;

    @Option(names = { "-f", "--force"},
            description = "Allow resubmission of previously submitted SIPs")
    private boolean force;

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getGroups() {
        return groups;
    }

    public void setGroups(String groups) {
        this.groups = groups;
    }

    public String getBrokerUrl() {
        return brokerUrl;
    }

    public void setBrokerUrl(String brokerUrl) {
        this.brokerUrl = brokerUrl;
    }

    public String getJmsEndpoint() {
        return jmsEndpoint;
    }

    public void setJmsEndpoint(String jmsEndpoint) {
        this.jmsEndpoint = jmsEndpoint;
    }

    public List<String> getSipIds() {
        return sipIds;
    }

    public void setSipIds(List<String> sipIds) {
        this.sipIds = sipIds;
    }

    public boolean isForce() {
        return force;
    }

    public void setForce(boolean force) {
        this.force = force;
    }
}
