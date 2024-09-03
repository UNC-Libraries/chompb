package edu.unc.lib.boxc.migration.cdm.test;

import org.apache.sshd.common.util.io.resource.PathResource;
import org.apache.sshd.common.util.security.SecurityUtils;
import org.apache.sshd.scp.server.ScpCommandFactory;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.server.channel.ChannelSession;
import org.apache.sshd.server.command.Command;
import org.apache.sshd.server.command.CommandFactory;
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider;
import org.apache.sshd.server.shell.ProcessShellFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyPair;

/**
 * SSH server used for testing
 *
 * @author bbpennel
 */
public class TestSshServer {
    public static final String PASSWORD = "supersecret";
    private SshServer sshServer;
    private Path clientKeyPath;

    public TestSshServer() throws IOException {
        sshServer = SshServer.setUpDefaultServer();
        sshServer.setHost("127.0.0.1");
        sshServer.setPort(42222);
        sshServer.setKeyPairProvider(new SimpleGeneratorHostKeyProvider());
        sshServer.setCommandFactory(new DelegatingCommandFactory());
        sshServer.setPasswordAuthenticator((username, password, serverSession) -> {
            return username != null && PASSWORD.equals(password);
        });
        sshServer.setPublickeyAuthenticator((username, key, session) -> {
            try {
                KeyPair clientKeyPair = SecurityUtils.loadKeyPairIdentities(
                        null,
                        new PathResource(clientKeyPath),
                        Files.newInputStream(clientKeyPath),
                        null
                ).iterator().next();

                return key.equals(clientKeyPair.getPublic());
            } catch (IOException | GeneralSecurityException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void setClientKeyPath(Path clientKeyPath) {
        this.clientKeyPath = clientKeyPath;
    }

    public void startServer() throws IOException {
        sshServer.start();
    }

    public void stopServer() throws IOException {
        sshServer.stop();
    }

    public class DelegatingCommandFactory implements CommandFactory {

        private final CommandFactory scpCommandFactory;
        private final CommandFactory shellCommandFactory;

        public DelegatingCommandFactory() {
            this.scpCommandFactory = new ScpCommandFactory();
            this.shellCommandFactory = (channel, command) -> new ProcessShellFactory(command, command.split(" ")).createShell(channel);
        }

        @Override
        public Command createCommand(ChannelSession channel, String command) throws IOException {
            if (command.startsWith("scp")) {
                return scpCommandFactory.createCommand(channel, command);
            } else {
                return shellCommandFactory.createCommand(channel, command);
            }
        }
    }
}
