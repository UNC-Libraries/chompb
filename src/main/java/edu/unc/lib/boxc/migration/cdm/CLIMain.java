package edu.unc.lib.boxc.migration.cdm;

import edu.unc.lib.boxc.migration.cdm.options.Verbosity;
import edu.unc.lib.boxc.migration.cdm.services.ChompbConfigService;
import edu.unc.lib.boxc.migration.cdm.util.BannerUtility;
import edu.unc.lib.boxc.migration.cdm.util.CLIConstants;
import org.apache.commons.lang3.StringUtils;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ScopeType;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Callable;

/**
 * Main class for the CLI utils
 *
 * @author bbpennel
 *
 */
@Command(subcommands = {
        InitializeProjectCommand.class,
        CdmFieldsCommand.class,
        CdmExportCommand.class,
        CdmIndexCommand.class,
        DestinationsCommand.class,
        SourceFilesCommand.class,
        AccessFilesCommand.class,
        DescriptionsCommand.class,
        SipsCommand.class,
        SubmitSipsCommand.class,
        StatusCommand.class,
        GroupMappingCommand.class,
        IndexRedirectCommand.class,
        ProjectPropertiesCommand.class,
        VerifyPostMigrationCommand.class,
        MigrationTypeReportCommand.class,
        FilterIndexCommand.class,
        AggregateFilesCommand.class,
        PermissionsCommand.class,
        ExportObjectsCommand.class,
        ListProjectsCommand.class,
        ArchiveProjectsCommand.class,
        ProcessSourceFilesCommand.class
    })
public class CLIMain implements Callable<Integer> {
    @Option(names = { "-w", "--work-dir" },
            scope = ScopeType.INHERIT,
            description = "Directory which the operations will happen relative to. Defaults to the current directory")
    private String workingDirectory;

    @Option(names = { "-v", "--verbose" },
            scope = ScopeType.INHERIT,
            description = "Set output to verbose level of verbosity")
    private boolean verbose;

    @Option(names = { "-q", "--quiet" },
            scope = ScopeType.INHERIT,
            description = "Set output to quiet level of verbosity")
    private boolean quiet;

    @Option(names = { "--env-config" },
            description = "Path to the chompb environment configuration file. Default: ${DEFAULT-VALUE}",
            defaultValue = "${env:ENV_CONFIG:-${sys:ENV_CONFIG:-}}")
    private Path envConfigPath;

    private ChompbConfigService.ChompbConfig chompbConfig;

    /**
     * @return Get the effective working directory
     */
    protected Path getWorkingDirectory() {
        Path currentPath = Paths.get(workingDirectory == null ? "." : workingDirectory);
        return currentPath.toAbsolutePath().normalize();
    }

    /**
     * @return Verbosity level to use with this command
     */
    protected Verbosity getVerbosity() {
        if (verbose) {
            return Verbosity.VERBOSE;
        }
        if (quiet) {
            return Verbosity.QUIET;
        }
        return Verbosity.NORMAL;
    }

    /**
     * @return Application configuration
     */
    public ChompbConfigService.ChompbConfig getChompbConfig() throws IOException {
        if (chompbConfig == null) {
            if (StringUtils.isBlank(envConfigPath.toString())) {
                throw new IllegalArgumentException("Must provide an env-config option");
            }
            var configService = new ChompbConfigService();
            configService.setEnvConfigPath(envConfigPath);
            chompbConfig = configService.getConfig();
        }
        return chompbConfig;
    }

    protected CLIMain() {
    }

    @Override
    public Integer call() throws Exception {
        CLIConstants.outputLogger.info(BannerUtility.getBanner());
        return 0;
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new CLIMain()).execute(args);
        System.exit(exitCode);
    }

}
