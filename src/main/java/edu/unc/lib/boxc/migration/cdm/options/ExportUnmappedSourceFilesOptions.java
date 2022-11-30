package edu.unc.lib.boxc.migration.cdm.options;

import picocli.CommandLine;

/**
 * Options for CDM export of unmapped source files operation
 *
 * @author bbpennel
 */
public class ExportUnmappedSourceFilesOptions {
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
}
