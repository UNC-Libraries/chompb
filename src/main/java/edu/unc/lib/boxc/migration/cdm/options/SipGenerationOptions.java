package edu.unc.lib.boxc.migration.cdm.options;

import picocli.CommandLine.Option;

/**
 * Options for sip generation operations
 * @author bbpennel
 */
public class SipGenerationOptions {

    @Option(names = { "-f", "--force"},
            description = { "Objects which are missing required details will be excluded from the SIP.",
                    "Normally the SIP generation operation would fail.",
                    "This includes missing source files and MODS descriptions."})
    private boolean force;

    @Option(names = { "-u", "--user"},
            description = {"Username of user performing the SIP, which will be recorded in PREMIS.",
                    "Defaults to current user: ${DEFAULT-VALUE}"},
            defaultValue = "${sys:user.name}")
    private String username;

    @Option(names = {"-sc", "--suppress-collection-redirect"},
            description = {"Suppress collection level redirects"})
    private boolean suppressCollectionRedirect;

    public boolean isForce() {
        return force;
    }

    public void setForce(boolean force) {
        this.force = force;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public boolean isSuppressCollectionRedirect() {
        return suppressCollectionRedirect;
    }

    public void setSuppressCollectionRedirect(boolean suppressCollectionRedirect) {
        this.suppressCollectionRedirect = suppressCollectionRedirect;
    }
}
