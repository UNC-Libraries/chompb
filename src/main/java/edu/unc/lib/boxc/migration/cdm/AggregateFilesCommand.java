package edu.unc.lib.boxc.migration.cdm;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.AggregateFileMappingOptions;
import edu.unc.lib.boxc.migration.cdm.options.Verbosity;
import edu.unc.lib.boxc.migration.cdm.services.AggregateFileMappingService;
import edu.unc.lib.boxc.migration.cdm.services.CdmIndexService;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.status.SourceFilesSummaryService;
import edu.unc.lib.boxc.migration.cdm.validators.AggregateFilesValidator;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static edu.unc.lib.boxc.migration.cdm.model.MigrationProject.AGGREGATE_BOTTOM_MAPPING_FILENAME;
import static edu.unc.lib.boxc.migration.cdm.model.MigrationProject.AGGREGATE_TOP_MAPPING_FILENAME;
import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author bbpennel
 */
@CommandLine.Command(name = "aggregate_files",
        description = "Commands related to aggregate file mappings")
public class AggregateFilesCommand {
    private static final Logger log = getLogger(AggregateFilesCommand.class);

    @CommandLine.ParentCommand
    private CLIMain parentCommand;

    private MigrationProject project;
    private AggregateFileMappingService aggregateService;
    private CdmIndexService indexService;
    private SourceFilesSummaryService summaryService;

    @CommandLine.Command(name = "generate",
            description = {
                    "Generate an aggregate file mapping for this project, mapping compound or grouped works to files.",
                    "By default will produce a mapping for files to add to the top of a work.",
                    "To add aggregate files to the bottom of works, use the --sort-bottom flag.",
                    "Individual projects can include both top and bottom mappings. Respectively, they are stored to "
                            + AGGREGATE_TOP_MAPPING_FILENAME + " and " + AGGREGATE_BOTTOM_MAPPING_FILENAME + ".",
                    "If multiple files are mapped to the same object across separate runs, they will be sorted in "
                            + "order added, either at the top or bottom of the work. "
                            + "So, earlier added sorts before later within each section.",
                    "Mappings are produced by listing files from a directory using the --base-path option, "
                            + "then searching for matches between those filenames and some filename field in the "
                            + "exported CDM records.",
                    "The filename field is set using the --field-name option.",
                    "If the value of the filename field does not match the name of the source file, the filename "
                            + " can be transformed using regular expressions via the --field-pattern"
                            + " and --field-pattern options.",
                    "The resulting will be written to the source_files.csv for this project, unless "
                            + "the --dry-run flag is provided."})
    public int generate(@CommandLine.Mixin AggregateFileMappingOptions options) throws Exception {
        long start = System.nanoTime();

        try {
            validateOptions(options);
            initialize(options.isSortBottom(), options.getDryRun());

            aggregateService.generateMapping(options);
            if (options.getDryRun()) {
                summaryService.summary(Verbosity.NORMAL);
            }
            outputLogger.info("Aggregate file mapping generated for {} in {}s", project.getProjectName(),
                    (System.nanoTime() - start) / 1e9);
            return 0;
        } catch (MigrationException | IllegalArgumentException e) {
            outputLogger.info("Cannot generate aggregate mapping: {}", e.getMessage());
            return 1;
        } catch (Exception e) {
            log.error("Failed to map aggregate files", e);
            outputLogger.info("Failed to map aggregate files: {}", e.getMessage(), e);
            return 1;
        }
    }

    @CommandLine.Command(name = "validate",
            description = "Validate a aggregate file mappings for this project. Defaults to top mapping.")
    public int validate(@CommandLine.Option(names = { "-f", "--force" },
                                description = "Ignore incomplete mappings") boolean force,
                        @CommandLine.Option(names = { "--sort-bottom" },
                                description = "Validate bottom sort mapping") boolean sortBottom) throws Exception {
        String mappingName = (sortBottom ? "Bottom" : "Top") + " aggregate file mappings";
        try {
            initialize(sortBottom, false);
            var validator = new AggregateFilesValidator(sortBottom);
            validator.setProject(project);
            List<String> errors = validator.validateMappings(force);

            var mappingPath = sortBottom ? project.getAggregateBottomMappingPath()
                    : project.getAggregateTopMappingPath();
            if (errors.isEmpty()) {
                outputLogger.info("PASS: {} at path {} is valid",
                        mappingName, mappingPath);
                return 0;
            } else {
                if (parentCommand.getVerbosity().equals(Verbosity.QUIET)) {
                    outputLogger.info("FAIL: {} is invalid with {} errors",
                            mappingName, errors.size());
                } else {
                    outputLogger.info("FAIL: {} at path {} is invalid due to the following issues:",
                            mappingName, mappingPath);
                    for (String error : errors) {
                        outputLogger.info("    - " + error);
                    }
                }
                return 1;
            }
        } catch (MigrationException e) {
            log.error("Failed to validate {}", mappingName, e);
            outputLogger.info("FAIL: Failed to validate {}: {}", e.getMessage());
            return 1;
        }
    }

    private void validateOptions(AggregateFileMappingOptions options) {
        if (options.getBasePath() == null) {
            throw new IllegalArgumentException("Must provide a base path or provide the --blank flag");
        }
        if (StringUtils.isBlank(options.getExportField())) {
            throw new IllegalArgumentException("Must provide an export field");
        }
    }

    private void initialize(boolean sortBottom, boolean dryRun) throws IOException {
        Path currentPath = parentCommand.getWorkingDirectory();
        project = MigrationProjectFactory.loadMigrationProject(currentPath);
        indexService = new CdmIndexService();
        indexService.setProject(project);
        summaryService = new SourceFilesSummaryService();
        summaryService.setProject(project);
        summaryService.setDryRun(dryRun);
        aggregateService = new AggregateFileMappingService(sortBottom);
        aggregateService.setIndexService(indexService);
        aggregateService.setProject(project);
    }
}
