package edu.unc.lib.boxc.migration.cdm.options;

import edu.unc.lib.boxc.auth.api.UserRole;
import picocli.CommandLine.Option;

public class PermissionMappingOptions {
    @Option(names = {"-id", "--cdm-id"},
            description = "CDM ID(s) of the item(s) being mapped")
    private String cdmId;

    @Option(names = {"-e", "--everyone"},
            description = "The patron access role assigned to the “everyone” group.")
    private UserRole everyone;

    @Option(names = {"-a", "--authenticated"},
            description = "The patron access role assigned to the “authenticated” group (anyone that is logged in).")
    private UserRole authenticated;

    @Option(names = {"-f", "--force"},
            description = "Overwrite permission mapping if one already exists")
    private boolean force;

    @Option(names = {"-so", "--staff-only"},
            description = "Staff only permissions, 'everyone' field and 'authenticated' field set to 'none'")
    private boolean staffOnly;

    public String getCdmId() {
        return cdmId;
    }

    public void setCdmId(String cdmId) {
        this.cdmId = cdmId;
    }

    public UserRole getEveryone() {
        return everyone;
    }

    public void setEveryone(UserRole everyone) {
        this.everyone = everyone;
    }

    public UserRole getAuthenticated() {
        return authenticated;
    }

    public void setAuthenticated(UserRole authenticated) {
        this.authenticated = authenticated;
    }

    public boolean isForce() {
        return force;
    }

    public void setForce(boolean force) {
        this.force = force;
    }

    public boolean isStaffOnly() {
        return staffOnly;
    }

    public void setStaffOnly(boolean staffOnly) {
        this.staffOnly = staffOnly;
    }
}
