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
package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.test.BxcEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.SipServiceHelper;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Paths;
import java.util.Arrays;

import static edu.unc.lib.boxc.migration.cdm.test.PostMigrationReportTestHelper.assertContainsRow;
import static edu.unc.lib.boxc.migration.cdm.test.PostMigrationReportTestHelper.parseReport;
import static org.mockito.MockitoAnnotations.initMocks;

/**
 * @author bbpennel
 */
public class PostMigrationReportServiceTest {
    private static final String BOXC_ID_1 = "bb3b83d7-2962-4604-a7d0-9afcb4ec99b1";
    private static final String BOXC_ID_2 = "91c08272-260f-40f1-bb7c-78854d504368";
    private static final String BOXC_ID_3 = "f9d7262c-3cfb-4d27-8ecc-b9df9ac2f950";
    @Rule
    public final TemporaryFolder tmpFolder = new TemporaryFolder();

    private MigrationProject project;
    private SipServiceHelper testHelper;
    private DescriptionsService descriptionsService;
    private PostMigrationReportService service;

    @Before
    public void setup() throws Exception {
        initMocks(this);
        tmpFolder.create();
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder.getRoot().toPath(), "proj", null, "user", CdmEnvironmentHelper.DEFAULT_ENV_ID);
        project.getProjectProperties().setBxcEnvironmentId(BxcEnvironmentHelper.DEFAULT_ENV_ID);
        ProjectPropertiesSerialization.write(project);

        testHelper = new SipServiceHelper(project, tmpFolder.newFolder().toPath());
        descriptionsService = testHelper.getDescriptionsService();

        service = new PostMigrationReportService();
        service.setProject(project);
        service.setChompbConfig(testHelper.getChompbConfig());
        service.setDescriptionsService(testHelper.getDescriptionsService());
        service.init();
    }

    @Test
    public void addSingleItemTest() throws Exception {
        testHelper.populateDescriptions("gilmer_mods1.xml");

        service.addWorkRow("25", BOXC_ID_1, 1, true);
        service.addFileRow("25/original_file", "25", BOXC_ID_1, BOXC_ID_2, true);
        service.closeCsv();

        var rows = parseReport(project);
        assertContainsRow(rows, "25",
                "http://localhost/cdm/singleitem/collection/proj/id/25",
                "Work",
                "http://localhost/bxc/record/" + BOXC_ID_1,
                "Redoubt C",
                "",
                "",
                "",
                "1");
        assertContainsRow(rows, "25/original_file",
                "http://localhost/cdm/singleitem/collection/proj/id/25",
                "File",
                "http://localhost/bxc/record/" + BOXC_ID_2,
                "",
                "",
                "http://localhost/bxc/record/" + BOXC_ID_1,
                "Redoubt C",
                "");
    }

    @Test
    public void addSingleItemWithFileDescTest() throws Exception {
        testHelper.populateDescriptions("gilmer_mods1.xml", "gilmer_mods_children.xml");

        service.addWorkRow("25", BOXC_ID_1, 1, true);
        service.addFileRow("25/original_file", "25", BOXC_ID_1, BOXC_ID_2, true);
        service.closeCsv();

        var rows = parseReport(project);
        assertContainsRow(rows, "25",
                "http://localhost/cdm/singleitem/collection/proj/id/25",
                "Work",
                "http://localhost/bxc/record/" + BOXC_ID_1,
                "Redoubt C",
                "",
                "",
                "",
                "1");
        assertContainsRow(rows, "25/original_file",
                "http://localhost/cdm/singleitem/collection/proj/id/25",
                "File",
                "http://localhost/bxc/record/" + BOXC_ID_2,
                "Redoubt C Scan File",
                "",
                "http://localhost/bxc/record/" + BOXC_ID_1,
                "Redoubt C",
                "");
    }

    @Test
    public void addGroupedTest() throws Exception {
        testHelper.populateDescriptions("grouped_mods.xml");

        service.addWorkRow("grp:groupa:group1", BOXC_ID_1, 2, false);
        service.addFileRow("26", "grp:groupa:group1", BOXC_ID_1, BOXC_ID_2, false);
        service.addFileRow("27", "grp:groupa:group1", BOXC_ID_1, BOXC_ID_3, false);
        service.closeCsv();

        var rows = parseReport(project);
        assertContainsRow(rows, "grp:groupa:group1",
                "",
                "Work",
                "http://localhost/bxc/record/" + BOXC_ID_1,
                "Folder Group 1",
                "",
                "",
                "",
                "2");
        assertContainsRow(rows, "26",
                "http://localhost/cdm/singleitem/collection/proj/id/26",
                "File",
                "http://localhost/bxc/record/" + BOXC_ID_2,
                "Plan of Battery McIntosh",
                "",
                "http://localhost/bxc/record/" + BOXC_ID_1,
                "Folder Group 1",
                "");
        assertContainsRow(rows, "27",
                "http://localhost/cdm/singleitem/collection/proj/id/27",
                "File",
                "http://localhost/bxc/record/" + BOXC_ID_3,
                "Fort DeRussy on Red River, Louisiana",
                "",
                "http://localhost/bxc/record/" + BOXC_ID_1,
                "Folder Group 1",
                "");
    }

    @Test
    public void addCompoundTest() throws Exception {
        testHelper.indexExportData(Paths.get("src/test/resources/keepsakes_fields.csv"), "mini_keepsakes");
        descriptionsService.generateDocuments(true);
        descriptionsService.expandDescriptions();

        service.addWorkRow("605", BOXC_ID_1, 2, false);
        service.addFileRow("602", "605", BOXC_ID_1, BOXC_ID_2, false);
        service.addFileRow("603", "605", BOXC_ID_1, BOXC_ID_3, false);
        service.closeCsv();

        var rows = parseReport(project);
        assertContainsRow(rows, "605",
                "http://localhost/cdm/compoundobject/collection/proj/id/605",
                "Work",
                "http://localhost/bxc/record/" + BOXC_ID_1,
                "Tiffany's pillbox commemorating UNC's bicentennial (closed, in box)",
                "",
                "",
                "",
                "2");
        assertContainsRow(rows, "602",
                "http://localhost/cdm/singleitem/collection/proj/id/602",
                "File",
                "http://localhost/bxc/record/" + BOXC_ID_2,
                "World War II ration book",
                "",
                "http://localhost/bxc/record/" + BOXC_ID_1,
                "Tiffany's pillbox commemorating UNC's bicentennial (closed, in box)",
                "");
        assertContainsRow(rows, "603",
                "http://localhost/cdm/singleitem/collection/proj/id/603",
                "File",
                "http://localhost/bxc/record/" + BOXC_ID_3,
                "World War II ration book (instructions)",
                "",
                "http://localhost/bxc/record/" + BOXC_ID_1,
                "Tiffany's pillbox commemorating UNC's bicentennial (closed, in box)",
                "");
    }
}
