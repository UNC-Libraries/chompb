package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.DestinationMappingOptions;
import edu.unc.lib.boxc.migration.cdm.test.BxcEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.SipServiceHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;

public class ArchivalDestinationsServiceTest {
    private static final String PROJECT_NAME = "proj";

    @TempDir
    public Path tmpFolder;

    private SipServiceHelper testHelper;
    private MigrationProject project;
    private ArchivalDestinationsService service;

    @BeforeEach
    public void setup() throws Exception {
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder, PROJECT_NAME, null, "user",
                CdmEnvironmentHelper.DEFAULT_ENV_ID, BxcEnvironmentHelper.DEFAULT_ENV_ID);
        testHelper = new SipServiceHelper(project, tmpFolder);
        service = new ArchivalDestinationsService();
        service.setProject(project);
        service.setIndexService(testHelper.getIndexService());
    }

    @Test
    public void archivalCollectionNumberTest() throws Exception {
        testHelper.indexExportData("mini_keepsakes");

        var options = new DestinationMappingOptions();
        options.setFieldName("dmrecord");

        var result = service.generateCollectionNumbersList(options);
        assertIterableEquals(Arrays.asList("216", "604", "607"), result);
    }
}
