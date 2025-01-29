package edu.unc.lib.boxc.migration.cdm.options;

/**
 * Interface for generate file mapping options
 * @author krwong
 */
public interface GenerateFileMappingOptions {
    boolean getDryRun();
    boolean getUpdate();
    boolean isForce();
}
