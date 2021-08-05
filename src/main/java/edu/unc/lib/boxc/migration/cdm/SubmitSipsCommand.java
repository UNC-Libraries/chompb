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

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.Callable;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import edu.unc.lib.boxc.deposit.impl.model.DepositStatusFactory;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.SipSubmissionOptions;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.services.SipService;
import edu.unc.lib.boxc.migration.cdm.services.SipSubmissionService;
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

        submissionService = new SipSubmissionService();
        submissionService.setProject(project);
        submissionService.setSipService(sipService);
        submissionService.setDepositStatusFactory(depositStatusFactory);
    }
}
