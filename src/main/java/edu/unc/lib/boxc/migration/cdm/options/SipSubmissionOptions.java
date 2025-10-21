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

    @Option(names = { "--redis-host"},
            description = { "Host for the Redis server.",
                "Defaults to using the REDIS_HOST variable: ${DEFAULT-VALUE}" },
            defaultValue = "${env:REDIS_HOST:-${sys:REDIS_HOST:-localhost}}")
    private String redisHost;

    @Option(names = { "--redis-port"},
            description = { "Port for the Redis server.",
                "Defaults to using the REDIS_PORT variable: ${DEFAULT-VALUE}" },
            defaultValue = "${env:REDIS_PORT:-${sys:REDIS_PORT:-6379}}")
    private int redisPort;

    @Option(names = { "--broker-url"},
            description = { "URL of the JMS broker to use for submitting to the deposit pipeline.",
                    "Defaults to using the BROKER_URL variable: ${DEFAULT-VALUE}" },
            defaultValue = "${env:BROKER_URL:-${sys:BROKER_URL:-tcp://localhost:61616}}")
    private String brokerUrl;

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

    public String getRedisHost() {
        return redisHost;
    }

    public void setRedisHost(String redisHost) {
        this.redisHost = redisHost;
    }

    public int getRedisPort() {
        return redisPort;
    }

    public void setRedisPort(int redisPort) {
        this.redisPort = redisPort;
    }

    public String getBrokerUrl() {
        return brokerUrl;
    }

    public void setBrokerUrl(String brokerUrl) {
        this.brokerUrl = brokerUrl;
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
