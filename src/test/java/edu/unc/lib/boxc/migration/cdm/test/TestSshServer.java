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
    public static final String PASSWORD = "supersecret";
    private SshServer sshServer;

    public TestSshServer() throws IOException {
        sshServer = SshServer.setUpDefaultServer();
        sshServer.setHost("127.0.0.1");
        sshServer.setPort(42222);
        sshServer.setKeyPairProvider(new SimpleGeneratorHostKeyProvider());
        sshServer.setCommandFactory(new ScpCommandFactory());
        sshServer.setPasswordAuthenticator((username, password, serverSession) -> {
            return username != null && PASSWORD.equals(password);
        });
    }

    public void startServer() throws IOException {
        sshServer.start();
    }

    public void stopServer() throws IOException {
        sshServer.stop();
    }
}
