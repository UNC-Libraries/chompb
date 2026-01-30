package edu.unc.lib.boxc.migration.cdm;

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import edu.unc.lib.boxc.migration.cdm.options.AddSourceFileMappingOptions;
import edu.unc.lib.boxc.migration.cdm.options.ExportUnmappedSourceFilesOptions;
import edu.unc.lib.boxc.migration.cdm.options.GenerateSourceFileMappingOptions;
import edu.unc.lib.boxc.migration.cdm.services.CdmExportFilesService;
import edu.unc.lib.boxc.migration.cdm.services.CdmFieldService;
import edu.unc.lib.boxc.migration.cdm.services.CdmFileRetrievalService;
import edu.unc.lib.boxc.migration.cdm.services.StreamingMetadataService;
import edu.unc.lib.boxc.migration.cdm.status.SourceFilesSummaryService;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.Verbosity;
import edu.unc.lib.boxc.migration.cdm.services.CdmIndexService;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.services.SourceFileService;
import edu.unc.lib.boxc.migration.cdm.status.SourceFilesStatusService;
import edu.unc.lib.boxc.migration.cdm.validators.SourceFilesValidator;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

/**
 * @author bbpennel
 */
@Command(name = "source_files",
        description = "Commands related to source file mappings")
public class SourceFilesCommand {
    private static final Logger log = getLogger(SourceFilesCommand.class);

    @ParentCommand
    private CLIMain parentCommand;

    private MigrationProject project;
    private SourceFileService sourceService;
    private CdmIndexService indexService;
    private CdmExportFilesService exportFilesService;
    private CdmFieldService fieldService;
    private SourceFilesSummaryService summaryService;
    private StreamingMetadataService streamingMetadataService;

    @Command(name = "generate",
            description = {
                    "Generate the source mapping file for this project.",
                    "Mappings are produced by listing files from a directory using the --base-path option, "
                    + "then searching for matches between those filenames and some filename field in the "
                    + "exported CDM records.",
                    "The filename field is set using the --field-name option.",
                    "If the value of the filename field does not match the name of the source file, the filename "
                    + " can be transformed using regular expressions via the --field-pattern"
                    + " and --field-pattern options.",
                    "The resulting will be written to the source_files.csv for this project, unless "
                    + "the --dry-run flag is provided."})
    public int generate(@Mixin GenerateSourceFileMappingOptions options) throws Exception {
        long start = System.nanoTime();

        try {
            validateOptions(options);
            initialize(options.getDryRun());

            summaryService.capturePreviousState();
            sourceService.generateMapping(options);
            summaryService.summary(parentCommand.getVerbosity());
            outputLogger.info("Source file mapping generated for {} in {}s", project.getProjectName(),
                    (System.nanoTime() - start) / 1e9);
            return 0;
        } catch (MigrationException | IllegalArgumentException e) {
            outputLogger.info("Cannot generate source mapping: {}", e.getMessage());
            return 1;
        } catch (Exception e) {
            log.error("Failed to map source files", e);
            outputLogger.info("Failed to map source files: {}", e.getMessage(), e);
            return 1;
        }
    }

    @Command(name = "validate",
            description = "Validate the source file mappings for this project")
    public int validate(@Option(names = { "-f", "--force"},
            description = "Ignore incomplete mappings") boolean force) throws Exception {
        try {
            initialize(false);
            SourceFilesValidator validator = new SourceFilesValidator();
            validator.setProject(project);
            validator.setStreamingMetadataService(streamingMetadataService);
            List<String> errors = validator.validateMappings(force);
            if (errors.isEmpty()) {
                outputLogger.info("PASS: Source file mapping at path {} is valid",
                        project.getSourceFilesMappingPath());
                return 0;
            } else {
                if (parentCommand.getVerbosity().equals(Verbosity.QUIET)) {
                    outputLogger.info("FAIL: Source file mapping is invalid with {} errors", errors.size());
                } else {
                    outputLogger.info("FAIL: Source file mapping at path {} is invalid due to the following issues:",
                            project.getSourceFilesMappingPath());
                    for (String error : errors) {
                        outputLogger.info("    - " + error);
                    }
                }
                return 1;
            }
        } catch (MigrationException e) {
            log.error("Failed to validate source file mappings", e);
            outputLogger.info("FAIL: Failed to validate source file mappings: {}", e.getMessage());
            return 1;
        }
    }

    @Command(name = "status",
            description = "Display status of the source file mappings for this project")
    public int status() throws Exception {
        try {
            initialize(false);
            SourceFilesStatusService statusService = new SourceFilesStatusService();
            statusService.setProject(project);
            statusService.setStreamingMetadataService(streamingMetadataService);
            statusService.report(parentCommand.getVerbosity());

            return 0;
        } catch (MigrationException | IllegalArgumentException e) {
            outputLogger.info("Status failed: {}", e.getMessage());
            return 1;
        } catch (Exception e) {
            log.error("Status failed", e);
            outputLogger.info("Status failed: {}", e.getMessage(), e);
            return 1;
        }
    }

    @Command(name = "add",
            description = {"Add files from the filesystem to the source mapping file for this project.",
                    "Mappings are produced by listing files from a directory using the --base-path option.",
                    "The user provides a list of file extensions field to include, extensions set using the " +
                            "--extensions option.",
                    "The user can set an optional prefix for chompb ids using the --optional-prefix option.",
                    "The resulting will be written to the source_files.csv for this project,"
                            + " unless the --dry-run flag is provided."})
    public int add(@Mixin AddSourceFileMappingOptions options) throws Exception {
        try {
            long start = System.nanoTime();
            initialize(options.getDryRun());
            sourceService.addToMapping(options);
            outputLogger.info("Source file mapping added for {} in {}s", project.getProjectName(),
                    (System.nanoTime() - start) / 1e9);
            return 0;
        } catch (MigrationException e) {
            outputLogger.info("Add source file mapping failed: {}", e.getMessage(), e);
            return 1;
        } catch (Exception e) {
            log.error("Add failed", e);
            outputLogger.info("Add source file mapping failed: {}", e.getMessage(), e);
            return 1;
        }
    }

    private void validateOptions(GenerateSourceFileMappingOptions options) {
        // If populating a blank mapping then other arguments not needed.
        if (options.isPopulateBlank()) {
            return;
        }
        if (options.getBasePath() == null) {
            throw new IllegalArgumentException("Must provide a base path or provide the --blank flag");
        }
        if (StringUtils.isBlank(options.getExportField())) {
            throw new IllegalArgumentException("Must provide an export field");
        }
    }

    private void initialize(boolean dryRun) throws IOException {
        Path currentPath = parentCommand.getWorkingDirectory();
        project = MigrationProjectFactory.loadMigrationProject(currentPath);
        fieldService = new CdmFieldService();
        indexService = new CdmIndexService();
        indexService.setProject(project);
        streamingMetadataService = new StreamingMetadataService();
        streamingMetadataService.setProject(project);
        streamingMetadataService.setFieldService(fieldService);
        streamingMetadataService.setIndexService(indexService);
        sourceService = new SourceFileService();
        sourceService.setIndexService(indexService);
        sourceService.setStreamingMetadataService(streamingMetadataService);
        sourceService.setProject(project);
        summaryService = new SourceFilesSummaryService();
        summaryService.setProject(project);
        summaryService.setDryRun(dryRun);
        summaryService.setSourceFileService(sourceService);
    }

    @Command(name = "export_unmapped",
            description = "Export files for any items which are listed in the mapping but have no source assigned")
    public int exportUnmapped(@Mixin ExportUnmappedSourceFilesOptions options) {
        try {
            long start = System.nanoTime();
            initializeExportFilesService(options);

            var result = exportFilesService.exportUnmapped();
            outputLogger.info("Finished downloading unmapped source files in {}s", (System.nanoTime() - start) / 1e9);
            if (result != null) {
                // Partial success with problems, output message to user
                outputLogger.info(result);
                return 2;
            } else {
                return 0;
            }
        } catch (MigrationException | IllegalArgumentException e) {
            outputLogger.info("Status failed: {}", e.getMessage());
            return 1;
        } catch (Exception e) {
            log.error("Status failed", e);
            outputLogger.info("Status failed: {}", e.getMessage(), e);
            return 1;
        }
    }

    private void initializeExportFilesService(ExportUnmappedSourceFilesOptions options) throws IOException {
        initialize(false);
        var fileRetrievalService = new CdmFileRetrievalService();
        fileRetrievalService.setChompbConfig(parentCommand.getChompbConfig());
        fileRetrievalService.setProject(project);
        fileRetrievalService.setSshUsername(options.getCdmUsername());
        fileRetrievalService.setSshPassword(options.getCdmPassword());
        exportFilesService = new CdmExportFilesService();
        exportFilesService.setIndexService(indexService);
        exportFilesService.setFileRetrievalService(fileRetrievalService);
        exportFilesService.setProject(project);
        exportFilesService.setSourceFileService(sourceService);
    }
}
