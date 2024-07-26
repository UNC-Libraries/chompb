package edu.unc.lib.boxc.migration.cdm.options;

import picocli.CommandLine;

/**
 * Options for aggregate file mapping
 * @author bbpennel
 */
public class AggregateFileMappingOptions extends GenerateSourceFileMappingOptions {
    @CommandLine.Option(names = { "--sort-bottom" },
            description = { "If specified, aggregate files mapped will be sorted after regular files in the work.",
                            "If not, then mapped files will be sorted before regular files in the work." } )
    private boolean sortBottom;

    public boolean isSortBottom() {
        return sortBottom;
    }

    public void setSortBottom(boolean sortBottom) {
        this.sortBottom = sortBottom;
    }
}
