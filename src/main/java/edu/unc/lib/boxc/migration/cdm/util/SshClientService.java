package edu.unc.lib.boxc.migration.cdm.util;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import org.apache.sshd.client.SshClient;
import org.apache.sshd.client.channel.ClientChannel;
import org.apache.sshd.client.channel.ClientChannelEvent;
import org.apache.sshd.client.session.ClientSession;
import org.apache.sshd.common.SshException;
import org.apache.sshd.common.config.keys.FilePasswordProvider;
import org.apache.sshd.common.keyprovider.KeyPairProvider;
import org.apache.sshd.common.util.io.resource.PathResource;
import org.apache.sshd.scp.client.ScpClient;
import org.apache.sshd.scp.client.ScpClientCreator;
import org.apache.sshd.common.util.security.SecurityUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.util.EnumSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static java.util.Collections.singletonList;

/**
 * Service for executing remote commands and transfers
 * @author bbpennel
 */
public class SshClientService {
    private static final int SSH_TIMEOUT_SECONDS = 10;

    private String sshHost;
    private int sshPort;
    private String sshUsername;
    private String sshPassword;
    private Path sshKeyPath;
    private KeyPair sshKeyPair;

    public void initialize() {
        if (sshKeyPath != null) {
            try {
                sshKeyPair = SecurityUtils.loadKeyPairIdentities(
                        null, new PathResource(sshKeyPath), Files.newInputStream(sshKeyPath), null
                ).iterator().next();
            } catch (IOException | GeneralSecurityException e) {
                throw new MigrationException("Failed to load ssh key", e);
            }
        }
    }

    private SshClient buildSshClient() {
        SshClient client = SshClient.setUpDefaultClient();
        if (sshKeyPair != null) {
            client.setKeyIdentityProvider(KeyPairProvider.wrap(singletonList(sshKeyPair)));
        } else if (sshPassword != null) {
            client.setFilePasswordProvider(FilePasswordProvider.of(sshPassword));
        }
        return client;
    }

    private void setupSessionAuthentication(ClientSession session) {
        if (sshKeyPair != null) {
            session.addPublicKeyIdentity(sshKeyPair);
        } else if (sshPassword != null) {
            session.addPasswordIdentity(sshPassword);
        }
    }

    /**
     * Execute a remote command on the server
     * @param command
     * @return Response output from the command
     */
    public String executeRemoteCommand(String command) {
        var response = new AtomicReference<String>();
        executeSshBlock(clientSession -> {
            response.set(executeRemoteCommand(clientSession, command));
        });
        return response.get();
    }

    /**
     * Execute a remote command on the server, using the provided session
     * @param command
     * @return Response output from the command
     */
    public String executeRemoteCommand(ClientSession clientSession, String command) {
        try (var responseStream = new ByteArrayOutputStream();
             ClientChannel channel = clientSession.createExecChannel(command)) {

            channel.setOut(responseStream);
            channel.setErr(responseStream);
            channel.open().verify(5, TimeUnit.SECONDS);

            channel.waitFor(EnumSet.of(ClientChannelEvent.CLOSED), 5000);
            return responseStream.toString();
        } catch (Exception e) {
            throw new MigrationException("Failed to execute remote command", e);
        }
    }

    /**
     * Execute a block of code with an SSH session
     * @param sshBlock
     */
    public void executeSshBlock(Consumer<ClientSession> sshBlock) {
        SshClient client = buildSshClient();
        client.start();
        try (var sshSession = client.connect(sshUsername, sshHost, sshPort)
                .verify(SSH_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .getSession()) {
            setupSessionAuthentication(sshSession);
            sshSession.auth().verify(SSH_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            sshBlock.accept(sshSession);
        } catch (IOException e) {
            if (e instanceof SshException && e.getMessage().contains("No more authentication methods available")) {
                throw new MigrationException("Authentication to server failed, check username or password", e);
            }
            throw new MigrationException("Failed to establish ssh session", e);
        }
    }

    /**
     * Execute a block of code that requires an SCP client
     * @param scpBlock
     */
    public void executeScpBlock(Consumer<ScpClient> scpBlock) {
        executeSshBlock(client -> {
            executeScpBlock(client, scpBlock);
        });
    }

    /**
     * Execute a block of code that requires an SCP client, using the provided ssh session
     * @param session
     * @param scpBlock
     */
    public void executeScpBlock(ClientSession session, Consumer<ScpClient> scpBlock) {
        var scpClientCreator = ScpClientCreator.instance();
        var scpClient = scpClientCreator.createScpClient(session);
        scpBlock.accept(scpClient);
    }

    public void setSshHost(String sshHost) {
        this.sshHost = sshHost;
    }

    public void setSshPort(int sshPort) {
        this.sshPort = sshPort;
    }

    public void setSshUsername(String sshUsername) {
        this.sshUsername = sshUsername;
    }

    public void setSshPassword(String sshPassword) {
        this.sshPassword = sshPassword;
    }

    public void setSshKeyPath(Path sshKeyPath) {
        this.sshKeyPath = sshKeyPath;
    }
}
