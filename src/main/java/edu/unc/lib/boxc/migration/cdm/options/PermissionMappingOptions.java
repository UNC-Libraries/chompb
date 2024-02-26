package edu.unc.lib.boxc.migration.cdm.options;

import edu.unc.lib.boxc.auth.api.UserRole;
import picocli.CommandLine.Option;

public class PermissionMappingOptions {
    @Option(names = {"-wd", "--with-default"},
            description = "Add 'default' entry to CSV mapping, 'id' field set to 'default")
    private boolean withDefault;

    @Option(names = {"-ww", "--with-works"},
            description = "Add entry for every work (grouped works, compound objects, and single file works) " +
                    "to CSV mapping")
    private boolean withWorks;

    @Option(names = {"-wf", "--with-files"},
            description = "Add entry for every file (compound children and grouped children) to CSV mapping")
    private boolean withFiles;

    @Option(names = {"-id", "--cdm-id"},
            description = "Work or File to add or update, can be cdmId or group work identifier")
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

    public boolean isWithDefault() {
        return withDefault;
    }

    public void setWithDefault(boolean withDefault) {
        this.withDefault = withDefault;
    }

    public boolean isWithWorks() {
        return withWorks;
    }

    public void setWithWorks(boolean withWorks) {
        this.withWorks = withWorks;
    }

    public boolean isWithFiles() {
        return withFiles;
    }

    public String getCdmId() {
        return cdmId;
    }

    public void setCdmId(String cdmId) {
        this.cdmId = cdmId;
    }

    public void setWithFiles(boolean withFiles) {
        this.withFiles = withFiles;
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
