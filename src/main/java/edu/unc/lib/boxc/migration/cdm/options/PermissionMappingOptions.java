package edu.unc.lib.boxc.migration.cdm.options;

import picocli.CommandLine.Option;

public class PermissionMappingOptions {
    @Option(names = {"-id", "--cdm-id"},
            description = {
                    "CDM ID(s) going to a specific box-c deposit destination in this migration project.",
                    "Must be a CDM ID or a comma-delimited list of CDM IDs"})
    private String cdmId;

    @Option(names = {"-e", "--everyone"},
            description = {
                    "CDM ID(s) going to a specific box-c deposit destination in this migration project.",
                    "Must be a CDM ID or a comma-delimited list of CDM IDs"})
    private String everyone;

    @Option(names = {"-a", "--authenticated"},
            description = {
                    "CDM ID(s) going to a specific box-c deposit destination in this migration project.",
                    "Must be a CDM ID or a comma-delimited list of CDM IDs"})
    private String authenticated;

    @Option(names = {"-f", "--force"},
            description = "Overwrite permission mapping if one already exists")
    private boolean force;

    @Option(names = {"-so", "--staff-only"},
            description = "Staff only permissions, 'everyone' field and 'authenticated' field set to 'none'.")
    private boolean staffOnly;

    public String getCdmId() {
        return cdmId;
    }

    public void setCdmId(String cdmId) {
        this.cdmId = cdmId;
    }

    public String getEveryone() {
        return everyone;
    }

    public void setEveryone(String everyone) {
        this.everyone = everyone;
    }

    public String getAuthenticated() {
        return authenticated;
    }

    public void setAuthenticated(String authenticated) {
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
