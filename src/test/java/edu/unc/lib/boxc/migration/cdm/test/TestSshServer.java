package edu.unc.lib.boxc.migration.cdm.test;

import org.apache.sshd.common.util.io.resource.PathResource;
import org.apache.sshd.common.util.security.SecurityUtils;
import org.apache.sshd.scp.server.ScpCommandFactory;
import org.apache.sshd.server.Environment;
import org.apache.sshd.server.ExitCallback;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.server.channel.ChannelSession;
import org.apache.sshd.server.command.Command;
import org.apache.sshd.server.command.CommandFactory;
import org.apache.sshd.server.command.CommandLifecycle;
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider;
import org.apache.sshd.server.shell.ProcessShellFactory;
import org.apache.sshd.sftp.server.SftpSubsystemFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.util.Collections;

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
        SftpSubsystemFactory factory = new SftpSubsystemFactory();
        sshServer.setSubsystemFactories(Collections.singletonList(factory));
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
            } else if (command.startsWith("sbatch")) {
                return new SbatchCommand();
            } else {
                return shellCommandFactory.createCommand(channel, command);
            }
        }
    }

    public class SbatchCommand implements Command, CommandLifecycle {
        private OutputStream out;

        public SbatchCommand() {
        }

        @Override
        public void setInputStream(java.io.InputStream in) { }

        @Override
        public void setOutputStream(OutputStream out) {
            this.out = out;
        }

        @Override
        public void setErrorStream(java.io.OutputStream err) { }

        @Override
        public void start(ChannelSession channel, Environment env) throws IOException {
            // Simulate successful sbatch job submission
            out.write(("Submitted batch job 123456\n").getBytes());
            out.flush();
            out.close();
        }

        @Override
        public void destroy(ChannelSession channel) throws Exception {
        }

        @Override
        public void setExitCallback(ExitCallback callback) {
            // Exit callback handling (success)
            callback.onExit(0);
        }
    }
}
