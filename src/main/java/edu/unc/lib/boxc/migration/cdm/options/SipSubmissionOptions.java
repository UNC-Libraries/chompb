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
            description = { "Post for the Redis server.",
                "Defaults to using the REDIS_PORT variable: ${DEFAULT-VALUE}" },
            defaultValue = "${env:REDIS_PORT:-${sys:REDIS_PORT:-6379}}")
    private int redisPort;

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

    public List<String> getSipIds() {
        return sipIds;
    }

    public void setSipIds(List<String> sipIds) {
        this.sipIds = sipIds;
    }
}
