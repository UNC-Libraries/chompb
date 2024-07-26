package edu.unc.lib.boxc.migration.cdm.options;

import java.nio.file.Path;

import picocli.CommandLine.Option;

/**
 * Options for generating source file mappings
 *
 * @author bbpennel
 */
public class SourceFileMappingOptions {
    @Option(names = {"-b", "--base-path"},
            description = {
                    "Required. Base file path to search in for source files to match with.",
                    "By default, all files which are immediate children of base path will be used.",
                    "To change this behavior, use the -g option."})
    private Path basePath;

    @Option(names = {"-u", "--update"},
            description = {
                    "If provided, then any source file matches produced will be used to update an existing"
                            + " source file mapping file, instead of attempting to create a new one.",
                    "This can be used to build up the mapping in multiple passes"})
    private boolean update;

    @Option(names = {"-d", "--dry-run"},
            description = {
                    "If provided, then the output of the matching will be displayed in the console rather "
                    + "than written to file"})
    private boolean dryRun;

    public Path getBasePath() {
        return basePath;
    }

    public void setBasePath(Path basePath) {
        this.basePath = basePath;
    }

    public boolean getUpdate() {
        return update;
    }

    public void setUpdate(boolean update) {
        this.update = update;
    }

    public boolean getDryRun() {
        return dryRun;
    }

    public void setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
    }
}
