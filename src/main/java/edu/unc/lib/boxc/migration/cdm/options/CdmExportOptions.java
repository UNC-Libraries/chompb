package edu.unc.lib.boxc.migration.cdm.options;

import picocli.CommandLine;

/**
 * Options for CDM export operation
 *
 * @author bbpennel
 */
public class CdmExportOptions {
    @CommandLine.Option(names = { "-u", "--cdm-user"},
            description = {"User name for CDM requests.",
                    "Defaults to current user: ${DEFAULT-VALUE}"},
            defaultValue = "${sys:user.name}")
    private String cdmUsername;
    @CommandLine.Option(names = {"-p", "--cdm-password"},
            description = "Password for CDM requests. Required.",
            arity = "0..1",
            interactive = true)
    private String cdmPassword;
    @CommandLine.Option(names = { "-f", "--force"},
            description = "Force the export to restart from the beginning. Use if a previous export was started "
                    + "or completed, but you would like to begin the export again.")
    private boolean force;

    public String getCdmUsername() {
        return cdmUsername;
    }

    public void setCdmUsername(String cdmUsername) {
        this.cdmUsername = cdmUsername;
    }

    public String getCdmPassword() {
        return cdmPassword;
    }

    public void setCdmPassword(String cdmPassword) {
        this.cdmPassword = cdmPassword;
    }

    public boolean isForce() {
        return force;
    }

    public void setForce(boolean force) {
        this.force = force;
    }
}
