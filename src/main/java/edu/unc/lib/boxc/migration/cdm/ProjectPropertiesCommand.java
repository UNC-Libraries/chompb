package edu.unc.lib.boxc.migration.cdm;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.services.ProjectPropertiesService;
import org.slf4j.Logger;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

import java.util.List;
import java.util.Map;

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Command to update migration project properties
 * @author krwong
 */
@Command(name = "config",
        description = "Configure a CDM migration project.")
public class ProjectPropertiesCommand {
    private static final Logger log = getLogger(ProjectPropertiesCommand.class);
    @ParentCommand
    private CLIMain parentCommand;

    private ProjectPropertiesService propertiesService = new ProjectPropertiesService();

    @Command(name = "list",
            description = "List all configurable properties for the project.")
    public int listProperties() throws Exception {
        try {
            MigrationProject project = MigrationProjectFactory
                    .loadMigrationProject(parentCommand.getWorkingDirectory());
            propertiesService.setProject(project);
            Map<String, String> projectProperties = propertiesService.getProjectProperties();
            for (Map.Entry<String, String> entry : projectProperties.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                if (value == null) {
                    value = "<unset>";
                }
                outputLogger.info(key + ": " + value);
            }
            return 0;
        } catch (MigrationException | IllegalArgumentException e) {
            outputLogger.info("{}", e.getMessage());
            return 1;
        } catch (Exception e) {
            log.error("Failed to list project properties", e);
            outputLogger.info("Failed to list project properties", e.getMessage());
            return 1;
        }
    }

    @Command(name = "set",
            description = "Manually set project properties. You can only set one property at a time. " +
                    "Example: chompb config set collectionNumber 00ddd")
    public int setProperties(@Parameters(index = "0", paramLabel = "<setField>",
                                     description = "Project property to set") String setField,
                             @Parameters(index = "1", paramLabel = "<setValue>",
                                     description = "New project property value") String setValue) throws Exception {
        try {
            MigrationProject project = MigrationProjectFactory
                    .loadMigrationProject(parentCommand.getWorkingDirectory());
            propertiesService.setProject(project);
            propertiesService.setProperty(setField, setValue);
            outputLogger.info("Project property set");
            return 0;
        } catch (MigrationException | IllegalArgumentException e) {
            outputLogger.info("{}", e.getMessage());
            return 1;
        } catch (Exception e) {
            log.error("Failed to set project property", e);
            outputLogger.info("Failed to set project property", e.getMessage());
            return 1;
        }
    }
    @Command(name = "unset",
            description = "Manually unset/clear project properties. " +
                    "You can clear multiple properties at the same time. " +
                    "Example: chompb config unset hookId collectionNumber")
    public int unsetProperties(@Parameters(paramLabel = "<unsetField(s)>",
            description = "List of fields to unset") List<String> unsetFields) throws Exception {
        try {
            MigrationProject project = MigrationProjectFactory
                    .loadMigrationProject(parentCommand.getWorkingDirectory());
            propertiesService.setProject(project);
            propertiesService.unsetProperties(unsetFields);
            outputLogger.info("Project property(ies) unset");
            return 0;
        } catch (MigrationException | IllegalArgumentException e) {
            outputLogger.info("{}", e.getMessage());
            return 1;
        } catch (Exception e) {
            log.error("Failed to unset project property(ies)", e);
            outputLogger.info("Failed to unset project property(ies)", e.getMessage());
            return 1;
        }
    }

}