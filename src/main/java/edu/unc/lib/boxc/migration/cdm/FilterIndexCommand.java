package edu.unc.lib.boxc.migration.cdm;

import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.IndexFilteringOptions;
import edu.unc.lib.boxc.migration.cdm.services.CdmIndexService;
import edu.unc.lib.boxc.migration.cdm.services.IndexFilteringService;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import org.slf4j.Logger;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.Callable;

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;
import static org.apache.commons.collections4.CollectionUtils.isEmpty;
import static org.apache.commons.collections4.CollectionUtils.isNotEmpty;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Command for filtering a project's index database.
 *
 * @author bbpennel
 */
@CommandLine.Command(name = "filter_index",
        description = {
                "Reduce the entries in the cdm_index for this project to a subset of the original collection.",
                "This is done using either inclusions or exclusions based on values within an export field."})
public class FilterIndexCommand implements Callable<Integer> {
    private static final Logger log = getLogger(FilterIndexCommand.class);
    @CommandLine.ParentCommand
    private CLIMain parentCommand;
    private MigrationProject project;
    private CdmIndexService indexService;
    private IndexFilteringService indexFilteringService;
    @CommandLine.Mixin
    private IndexFilteringOptions options;

    @Override
    public Integer call() throws Exception {
        long start = System.nanoTime();

        try {
            validateOptions(options);
            initialize();
            assertProjectStateValid();

            var counts = indexFilteringService.calculateRemainder(options);
            if (counts.get("remainder") == 0) {
                outputLogger.info("Filter would remove all entries from the index, cancelling operation");
                return 1;
            }
            if (counts.get("remainder") == counts.get("total")) {
                outputLogger.info(
                        "Number of entries in the index ({}) would be unchanged by the filter, cancelling operation",
                        counts.get("total"));
                return 1;
            }
            outputLogger.info("Filtering index from {} to {} remaining entries",
                    counts.get("total"), counts.get("remainder"));
            if (options.isDryRun()) {
                outputLogger.info("== Dry run, no entries have been removed from the index ==");
                return 0;
            }
            indexFilteringService.filterIndex(options);
            outputLogger.info("Filtering of index for {} completed {}s", project.getProjectName(),
                    (System.nanoTime() - start) / 1e9);
            return 0;
        } catch (IllegalArgumentException e) {
            outputLogger.info(e.getMessage());
            return 1;
        } catch (Exception e) {
            log.error("Failed to filter index", e);
            outputLogger.info("Failed to filter index: {}", e.getMessage());
            return 1;
        }
    }

    private void validateOptions(IndexFilteringOptions options) {
        if (isNotEmpty(options.getExcludeValues()) && isNotEmpty(options.getIncludeValues())) {
            throw new IllegalArgumentException("Cannot provide both --include and --exclude at the same time");
        }
        if (isEmpty(options.getExcludeValues()) && isEmpty(options.getIncludeValues()) &&
                isBlank(options.getIncludeRangeStart()) && isBlank(options.getIncludeRangeEnd())
                && isBlank(options.getExcludeRangeStart()) && isBlank(options.getExcludeRangeEnd())) {
            throw new IllegalArgumentException("Must provide an --include, --exclude, --include-range-start and " +
                    "--include-range-end, or --exclude-range-start and --exclude range-end value(s) (but not all)");
        }
        if ((isNotBlank(options.getIncludeRangeStart()) || isNotBlank(options.getIncludeRangeEnd())) &&
                (isNotBlank(options.getExcludeRangeStart()) || isNotBlank(options.getExcludeRangeEnd()))) {
            throw new IllegalArgumentException("Cannot provide both --include-range and" +
                    " --exclude-range values at the same time");
        }
        if ((!isBlank(options.getIncludeRangeStart()) && isBlank(options.getIncludeRangeEnd())) ||
                (isBlank(options.getIncludeRangeStart()) && !isBlank(options.getIncludeRangeEnd()))) {
            throw new IllegalArgumentException("Must provide both --include-range-start and --include-range-end");
        }
        if ((!isBlank(options.getExcludeRangeStart()) && isBlank(options.getExcludeRangeEnd())) ||
                (isBlank(options.getExcludeRangeStart()) && !isBlank(options.getExcludeRangeEnd()))) {
            throw new IllegalArgumentException("Must provide both --exclude-range-start and --exclude-range-end");
        }
        if (isBlank(options.getFieldName())) {
            throw new IllegalArgumentException("Must provide a --field-name value");
        }
    }

    private void initialize() throws IOException {
        Path currentPath = parentCommand.getWorkingDirectory();
        project = MigrationProjectFactory.loadMigrationProject(currentPath);
        indexService = new CdmIndexService();
        indexService.setProject(project);
        indexFilteringService = new IndexFilteringService();
        indexFilteringService.setIndexService(indexService);
        indexFilteringService.setProject(project);
    }

    private void assertProjectStateValid() {
        if (project.getProjectProperties().getIndexedDate() == null) {
            throw new InvalidProjectStateException("Project must be indexed prior to filtering");
        }
    }
}
