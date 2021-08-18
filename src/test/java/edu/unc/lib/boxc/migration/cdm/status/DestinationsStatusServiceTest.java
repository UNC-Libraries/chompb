/**
 * Copyright 2008 The University of North Carolina at Chapel Hill
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.unc.lib.boxc.migration.cdm.status;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import edu.unc.lib.boxc.migration.cdm.AbstractOutputTest;
import edu.unc.lib.boxc.migration.cdm.model.DestinationsInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.Verbosity;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.test.SipServiceHelper;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;

/**
 * @author bbpennel
 */
public class DestinationsStatusServiceTest extends AbstractOutputTest {
    private static final String PROJECT_NAME = "proj";
    private static final String USERNAME = "migr_user";
    private static final String DEST_UUID = "bfe93126-849a-43a5-b9d9-391e18ffacc6";
    private static final String DEST_UUID2 = "8ae56bbc-400e-496d-af4b-3c585e20dba1";

    @Rule
    public final TemporaryFolder tmpFolder = new TemporaryFolder();

    private MigrationProject project;
    private SipServiceHelper testHelper;
    private DestinationsStatusService statusService;

    @Before
    public void setup() throws Exception {
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder.newFolder().toPath(), PROJECT_NAME, null, USERNAME);

        testHelper = new SipServiceHelper(project, tmpFolder.newFolder().toPath());
        statusService = new DestinationsStatusService();
        statusService.setProject(project);
    }

    @Test
    public void destinationsNotGeneratedTest() throws Exception {
        testHelper.indexExportData("export_1.xml");
        statusService.report(Verbosity.NORMAL);

        assertOutputMatches(".*Last Generated: +Not completed.*");
    }

    @Test
    public void destinationsNotValidTest() throws Exception {
        testHelper.indexExportData("export_1.xml");
        writeCsv(mappingBody("default,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,",
                             "27,3f3c5bcf-d5d6-46ad-,"));

        statusService.report(Verbosity.NORMAL);

        assertOutputMatches(".*Last Generated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Objects Mapped: +3 \\(100.0%\\).*");
        assertOutputMatches(".*Unmapped Objects: +0 \\(0.0%\\).*");
        assertOutputMatches(".*Unknown Objects: +0.*");
        assertOutputMatches(".*Destinations Valid: +No.*");
        assertOutputMatches(".*To Default: +2 \\(66.7%\\).*");
        assertOutputMatches(".*Destinations: +2\n.*");
        assertOutputMatches(".*Destinations:.*\n +\\* 3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e.*");
        assertOutputMatches(".*New Collections: +0\n.*");
    }

    @Test
    public void destinationsNotValidVerboseTest() throws Exception {
        testHelper.indexExportData("export_1.xml");
        writeCsv(mappingBody("default,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,",
                             "27,3f3c5bcf-d5d6-46ad-,"));

        statusService.report(Verbosity.VERBOSE);

        assertOutputMatches(".*Last Generated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Objects Mapped: +3 \\(100.0%\\).*");
        assertOutputMatches(".*Unmapped Objects: +0 \\(0.0%\\).*");
        assertOutputMatches(".*Unknown Objects: +0.*");
        assertOutputMatches(".*Destinations Valid: +No.*");
        assertOutputMatches(".*To Default: +2 \\(66.7%\\).*");
        assertOutputMatches(".*Destinations: +2\n.*");
        assertOutputMatches(".*Destinations:.*\n +\\* 3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e.*");
        assertOutputMatches(".*New Collections: +0\n.*");
        assertOutputMatches(".*Invalid destination at line 3, .* is not a valid UUID.*");
    }

    @Test
    public void destinationsNotValidQuietTest() throws Exception {
        testHelper.indexExportData("export_1.xml");
        writeCsv(mappingBody("default,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,",
                             "27,3f3c5bcf-d5d6-46ad-,"));

        statusService.report(Verbosity.QUIET);

        assertOutputMatches(".*Last Generated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Objects Mapped: +3 \\(100.0%\\).*");
        assertOutputMatches(".*Unmapped Objects: +0 \\(0.0%\\).*");
        assertOutputMatches(".*Unknown Objects: +0.*");
        assertOutputMatches(".*Destinations Valid: +No.*");
        assertOutputMatches(".*To Default: +2 \\(66.7%\\).*");
        assertOutputMatches(".*Destinations: +2\n.*");
        assertOutputNotMatches(".*Destinations:.*\n +\\* 3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e.*");
        assertOutputMatches(".*New Collections: +0\n.*");
    }

    @Test
    public void unmappedObjectsTest() throws Exception {
        testHelper.indexExportData("export_1.xml");
        writeCsv(mappingBody("26,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,",
                             "27,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,"));

        statusService.report(Verbosity.NORMAL);

        assertOutputMatches(".*Last Generated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Objects Mapped: +2 \\(66.7%\\).*");
        assertOutputMatches(".*Unmapped Objects: +1 \\(33.3%\\).*");
        assertOutputNotMatches(".*Unmapped Objects:.*\n +\\* 25\n.*");
        assertOutputMatches(".*Unknown Objects: +0.*");
        assertOutputMatches(".*Destinations Valid: +Yes.*");
        assertOutputMatches(".*To Default: +0 \\(0.0%\\).*");
        assertOutputMatches(".*Destinations: +1\n.*");
        assertOutputMatches(".*Destinations:.*\n +\\* 3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e.*");
        assertOutputMatches(".*New Collections: +0\n.*");
    }

    @Test
    public void unmappedObjectsVerboseTest() throws Exception {
        testHelper.indexExportData("export_1.xml");
        writeCsv(mappingBody("26,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,",
                             "27,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,"));

        statusService.report(Verbosity.VERBOSE);

        assertOutputMatches(".*Last Generated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Objects Mapped: +2 \\(66.7%\\).*");
        assertOutputMatches(".*Unmapped Objects: +1 \\(33.3%\\).*");
        assertOutputMatches(".*Unmapped Objects:.*\n +\\* 25\n.*");
        assertOutputMatches(".*Unknown Objects: +0.*");
        assertOutputMatches(".*Destinations Valid: +Yes.*");
        assertOutputMatches(".*To Default: +0 \\(0.0%\\).*");
        assertOutputMatches(".*Destinations: +1\n.*");
        assertOutputMatches(".*Destinations:.*\n +\\* 3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e.*");
        assertOutputMatches(".*New Collections: +0\n.*");
    }

    @Test
    public void unknownObjectsTest() throws Exception {
        testHelper.indexExportData("export_1.xml");
        writeCsv(mappingBody("25,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,",
                             "26,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,",
                             "27,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,",
                             "55,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,"));

        statusService.report(Verbosity.NORMAL);

        assertOutputMatches(".*Last Generated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Objects Mapped: +3 \\(100.0%\\).*");
        assertOutputMatches(".*Unmapped Objects: +0 \\(0.0%\\).*");
        assertOutputMatches(".*Unknown Objects: +1 .*");
        assertOutputNotMatches(".*Unknown Objects:.*\n +\\* 55\n.*");
        assertOutputMatches(".*Destinations Valid: +Yes.*");
        assertOutputMatches(".*To Default: +0 \\(0.0%\\).*");
        assertOutputMatches(".*Destinations: +1\n.*");
        assertOutputMatches(".*Destinations:.*\n +\\* 3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e.*");
        assertOutputMatches(".*New Collections: +0\n.*");
    }

    @Test
    public void unknownObjectsVerboseTest() throws Exception {
        testHelper.indexExportData("export_1.xml");
        writeCsv(mappingBody("25,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,",
                             "26,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,",
                             "27,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,",
                             "55,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,"));

        statusService.report(Verbosity.VERBOSE);

        assertOutputMatches(".*Last Generated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Objects Mapped: +3 \\(100.0%\\).*");
        assertOutputMatches(".*Unmapped Objects: +0 \\(0.0%\\).*");
        assertOutputMatches(".*Unknown Objects: +1 .*");
        assertOutputMatches(".*Unknown Objects:.*\n +\\* 55\n.*");
        assertOutputMatches(".*Destinations Valid: +Yes.*");
        assertOutputMatches(".*To Default: +0 \\(0.0%\\).*");
        assertOutputMatches(".*Destinations: +1\n.*");
        assertOutputMatches(".*Destinations:.*\n +\\* 3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e.*");
        assertOutputMatches(".*New Collections: +0\n.*");
    }

    @Test
    public void allMappedDefaultTest() throws Exception {
        testHelper.indexExportData("export_1.xml");
        writeCsv(mappingBody("default,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,001234"));

        statusService.report(Verbosity.NORMAL);

        assertOutputMatches(".*Last Generated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Objects Mapped: +3 \\(100.0%\\).*");
        assertOutputMatches(".*Unmapped Objects: +0 \\(0.0%\\).*");
        assertOutputMatches(".*Unknown Objects: +0.*");
        assertOutputMatches(".*Destinations Valid: +Yes.*");
        assertOutputMatches(".*To Default: +3 \\(100.0%\\).*");
        assertOutputMatches(".*Destinations: +1\n.*");
        assertOutputMatches(".*Destinations:.*\n +\\* 3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e\\|001234.*");
        assertOutputMatches(".*New Collections: +1\n.*");
        assertOutputMatches(".*New Collections:.*\n +\\* 001234.*");
    }

    @Test
    public void unpopulatedDestObjectsTest() throws Exception {
        testHelper.indexExportData("export_1.xml");
        writeCsv(mappingBody("25,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,",
                             "26,,",
                             "27,3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e,"));

        statusService.report(Verbosity.VERBOSE);

        assertOutputMatches(".*Last Generated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Objects Mapped: +2 \\(66.7%\\).*");
        assertOutputMatches(".*Unmapped Objects: +1 \\(33.3%\\).*");
        assertOutputMatches(".*Unmapped Objects:.*\n +\\* 26\n.*");
        assertOutputMatches(".*Unknown Objects: +0 .*");
        assertOutputMatches(".*Destinations Valid: +No.*");
        assertOutputMatches(".*To Default: +0 \\(0.0%\\).*");
        assertOutputMatches(".*Destinations: +1\n.*");
        assertOutputMatches(".*Destinations:.*\n +\\* 3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e.*");
        assertOutputMatches(".*New Collections: +0\n.*");
    }

    private String mappingBody(String... rows) {
        return String.join(",", DestinationsInfo.CSV_HEADERS) + "\n"
                + String.join("\n", rows);
    }

    private void writeCsv(String mappingBody) throws IOException {
        FileUtils.write(project.getDestinationMappingsPath().toFile(),
                mappingBody, StandardCharsets.UTF_8);
        project.getProjectProperties().setDestinationsGeneratedDate(Instant.now());
        ProjectPropertiesSerialization.write(project);
    }
}