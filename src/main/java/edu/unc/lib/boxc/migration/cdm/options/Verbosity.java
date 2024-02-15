package edu.unc.lib.boxc.migration.cdm.options;

/**
 * Level of verbosity
 * @author bbpennel
 */
public enum Verbosity {
    QUIET, NORMAL, VERBOSE;

    /**
     * @return True if the level is verbose or higher
     */
    public boolean isVerbose() {
        return this.equals(VERBOSE);
    }

    /**
     * @return True if the level is normal or higher
     */
    public boolean isNormal() {
        return this.equals(NORMAL) || this.equals(VERBOSE);
    }

    /**
     * @return True if the level is quiet
     */
    public boolean isQuiet() {
        return this.equals(QUIET);
    }
}
