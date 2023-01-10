package edu.unc.lib.boxc.migration.cdm.services;

import static edu.unc.lib.boxc.migration.cdm.services.ProjectPropertiesService.HOOK_ID;
import static edu.unc.lib.boxc.migration.cdm.services.ProjectPropertiesService.COLLECTION_NUMBER;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import org.junit.jupiter.api.io.TempDir;

/**
 * @author krwong
 */
public class ProjectPropertiesServiceTest {
    private static final String PROJECT_NAME = "gilmer";

    @TempDir
    public Path tmpFolder;

    private MigrationProject project;
    private ProjectPropertiesService service;

    @BeforeEach
    public void setup() throws Exception {
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder.getRoot(), PROJECT_NAME, null, "user", CdmEnvironmentHelper.DEFAULT_ENV_ID);
        service = new ProjectPropertiesService();
        service.setProject(project);
    }

    @Test
    public void listProjectPropertiesTest() throws Exception {
        Map<String, String> projectProperties = service.getProjectProperties();

        assertTrue(projectProperties.containsKey(HOOK_ID));
        assertTrue(projectProperties.containsValue(null));
        assertTrue(projectProperties.containsKey(COLLECTION_NUMBER));
    }

    @Test
    public void setProjectPropertiesTest() throws Exception {
        service.setProperty(HOOK_ID, "000dd");
        service.setProperty(COLLECTION_NUMBER, "000dd");

        assertEquals("000dd", project.getProjectProperties().getHookId());
        assertEquals("000dd", project.getProjectProperties().getCollectionNumber());
    }

    @Test
    public void setProjectPropertiesInvalidFieldTest() throws Exception {
        try {
            service.setProperty("ookId", "000dd");
            fail();
        } catch (MigrationException e) {
            assertTrue(e.getMessage().contains("Invalid field/value input"));
        }
    }

    @Test
    public void setProjectPropertyNullValueTest() throws Exception {
        try {
            service.setProperty(HOOK_ID, null);
            fail();
        } catch (MigrationException e) {
            assertTrue(e.getMessage().contains("Invalid field/value input"));
        }
    }

    @Test
    public void unsetProjectPropertiesTest() throws Exception {
        List<String> unsetFields = new ArrayList<>();
        unsetFields.add(HOOK_ID);
        unsetFields.add(COLLECTION_NUMBER);

        service.unsetProperties(unsetFields);

        assertNull(project.getProjectProperties().getHookId());
        assertNull(project.getProjectProperties().getCollectionNumber());
    }

    @Test
    public void unsetProjectPropertiesFailTest() throws Exception {
        List<String> unsetFields = new ArrayList<>();
        unsetFields.add("ookId");

        try {
            service.unsetProperties(unsetFields);
            fail();
        } catch (MigrationException e) {
            assertTrue(e.getMessage().contains("Invalid project property(ies)"));
        }
    }
}
