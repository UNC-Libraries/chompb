package edu.unc.lib.boxc.migration.cdm.options;

import picocli.CommandLine;

/**
 * Options for job to process source files
 * @author bbpennel
 */
public class ProcessSourceFilesOptions {
    @CommandLine.Option(names = {"-a", "--action"},
            description = "Name of the processing action to execute.")
    private String actionName;

    @CommandLine.Option(names = {"-u", "--user"},
            description = "Username of the user that started this job. Defaults to current user",
            defaultValue = "${sys:user.name}")
    private String username;

    @CommandLine.Option(names = {"-e", "--email"},
            description = "Email of the user that started this job")
    private String emailAddress;

    public String getActionName() {
        return actionName;
    }

    public void setActionName(String actionName) {
        this.actionName = actionName;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getEmailAddress() {
        return emailAddress;
    }

    public void setEmailAddress(String emailAddress) {
        this.emailAddress = emailAddress;
    }
}
