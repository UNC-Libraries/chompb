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
package edu.unc.lib.boxc.migration.cdm.test;

import org.apache.sshd.scp.server.ScpCommandFactory;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider;

import java.io.IOException;

/**
 * SSH server used for testing
 *
 * @author bbpennel
 */
public class TestSshServer {
    private SshServer sshServer;
    private String password;

    public TestSshServer() throws IOException {
        sshServer = SshServer.setUpDefaultServer();
        sshServer.setHost("127.0.0.1");
        sshServer.setPort(42222);
        sshServer.setKeyPairProvider(new SimpleGeneratorHostKeyProvider());
        sshServer.setCommandFactory(new ScpCommandFactory());
        sshServer.setPasswordAuthenticator((username, password, serverSession) -> {
            return username != null && password.equals(password);
        });
    }

    public void startServer() throws IOException {
        sshServer.start();
    }

    public void stopServer() throws IOException {
        sshServer.stop();
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
