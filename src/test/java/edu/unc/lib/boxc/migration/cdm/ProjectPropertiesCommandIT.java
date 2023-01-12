package edu.unc.lib.boxc.migration.cdm;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.services.ProjectPropertiesService;

/**
 * @author krwong
 */
public class ProjectPropertiesCommandIT extends AbstractCommandIT {
    private static final String PROJECT_NAME = "gilmer";

    private ProjectPropertiesService propertiesService;

    @BeforeEach
    public void setup() throws Exception {
        initProject();
        propertiesService = new ProjectPropertiesService();
        propertiesService.setProject(project);
    }

    @Test
    public void listProjectPropertiesTest() throws Exception {
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "config", "list"};
        executeExpectSuccess(args);

        assertOutputContains("hookId: <unset>\ncollectionNumber: <unset>\n");
    }

    @Test
    public void setProjectPropertiesTest() throws Exception {
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "config", "set", "collectionNumber", "000dd"};
        executeExpectSuccess(args);

        assertOutputContains("Project property set");
    }

    @Test
    public void setProjectPropertiesInvalidFieldTest() throws Exception {
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "config", "set", "ookId", "000dd"};
        executeExpectFailure(args);

        assertOutputContains("Invalid field/value input");
    }

    @Test
    public void setProjectPropertiesNoValueTest() throws Exception {
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "config", "set", "hookId"};
        executeExpectFailure(args);
    }

    @Test
    public void unsetOneProjectPropertyTest() throws Exception {
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "config", "unset", "hookId"};
        executeExpectSuccess(args);

        assertOutputContains("Project property(ies) unset");
    }

    @Test
    public void unsetMultipleProjectPropertiesTest() throws Exception {
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "config", "unset", "hookId", "collectionNumber"};
        executeExpectSuccess(args);

        assertOutputContains("Project property(ies) unset");
    }

    @Test
    public void unsetProjectPropertiesFailTest() throws Exception {
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "config", "unset", "hookId", "collection"};
        executeExpectFailure(args);

        assertOutputContains("Invalid project property(ies)");
    }

}
