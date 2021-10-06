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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.exceptions.StateAlreadyExistsException;
import edu.unc.lib.boxc.migration.cdm.model.GroupMappingInfo;
import edu.unc.lib.boxc.migration.cdm.model.GroupMappingInfo.GroupMapping;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProjectProperties;
import edu.unc.lib.boxc.migration.cdm.options.GroupMappingOptions;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;

/**
 * @author bbpennel
 */
public class GroupMappingServiceTest {
    private static final String PROJECT_NAME = "proj";

    @Rule
    public final TemporaryFolder tmpFolder = new TemporaryFolder();

    private MigrationProject project;
    private CdmIndexService indexService;
    private CdmFieldService fieldService;
    private GroupMappingService service;

    @Before
    public void setup() throws Exception {
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder.getRoot().toPath(), PROJECT_NAME, null, "user");
        Files.createDirectories(project.getExportPath());

        fieldService = new CdmFieldService();
        indexService = new CdmIndexService();
        indexService.setProject(project);
        indexService.setFieldService(fieldService);
        service = new GroupMappingService();
        service.setIndexService(indexService);
        service.setProject(project);
    }

    @Test
    public void generateNoIndexTest() throws Exception {
        GroupMappingOptions options = makeDefaultOptions();

        try {
            service.generateMapping(options);
            fail();
        } catch (InvalidProjectStateException e) {
            assertExceptionContains("Project must be indexed", e);
            assertMappedDateNotPresent();
        }
    }

    @Test
    public void generateSingleRunTest() throws Exception {
        indexExportSamples();
        GroupMappingOptions options = makeDefaultOptions();
        service.generateMapping(options);

        GroupMappingInfo info = service.loadMappings();
        assertMappingPresent(info, "25", "group1");
        assertMappingPresent(info, "26", "group1");
        assertMappingPresent(info, "27", "group2");
        assertMappingPresent(info, "28", null);
        assertMappingPresent(info, "29", null);
        assertEquals(5, info.getMappings().size());

        GroupMappingInfo groupedInfo  = service.loadGroupedMappings();
        assertGroupingPresent(groupedInfo, "group1", "25", "26");
        assertGroupingPresent(groupedInfo, "group2", "27");
        assertEquals(2, groupedInfo.getGroupedMappings().size());

        assertMappedDatePresent();
    }

    @Test
    public void generateDryRunTest() throws Exception {
        indexExportSamples();
        GroupMappingOptions options = makeDefaultOptions();
        options.setDryRun(true);
        service.generateMapping(options);

        try {
            service.loadMappings();
            fail();
        } catch (NoSuchFileException e) {
            // expected
        }

        assertMappedDateNotPresent();
    }

    @Test
    public void generateDoubleRunTest() throws Exception {
        indexExportSamples();
        GroupMappingOptions options = makeDefaultOptions();
        service.generateMapping(options);

        try {
            service.generateMapping(options);
            fail();
        } catch (StateAlreadyExistsException e) {
            // expected
        }

        // mapping state should be unchanged
        GroupMappingInfo info = service.loadMappings();
        assertMappingPresent(info, "25", "group1");
        assertMappingPresent(info, "26", "group1");
        assertMappingPresent(info, "27", "group2");
        assertMappingPresent(info, "28", null);
        assertMappingPresent(info, "29", null);
        assertEquals(5, info.getMappings().size());

        assertMappedDatePresent();
    }

    @Test
    public void generateSecondDryRunTest() throws Exception {
        indexExportSamples();
        GroupMappingOptions options = makeDefaultOptions();
        service.generateMapping(options);

        options.setDryRun(true);

        service.generateMapping(options);

        // mapping state should be unchanged
        GroupMappingInfo info = service.loadMappings();
        assertMappingPresent(info, "25", "group1");
        assertMappingPresent(info, "26", "group1");
        assertMappingPresent(info, "27", "group2");
        assertMappingPresent(info, "28", null);
        assertMappingPresent(info, "29", null);
        assertEquals(5, info.getMappings().size());

        assertMappedDatePresent();
    }

    @Test
    public void generateForceRunTest() throws Exception {
        indexExportSamples();
        GroupMappingOptions options = makeDefaultOptions();
        service.generateMapping(options);

        GroupMappingInfo info = service.loadMappings();
        assertMappingPresent(info, "25", "group1");
        assertMappingPresent(info, "26", "group1");
        assertMappingPresent(info, "27", "group2");
        assertMappingPresent(info, "28", null);
        assertMappingPresent(info, "29", null);
        assertEquals(5, info.getMappings().size());

        options.setForce(true);
        options.setGroupField("digitc");

        service.generateMapping(options);

        // Mapping state should have been overwritten
        GroupMappingInfo info2 = service.loadMappings();
        assertMappingPresent(info2, "25", "2005-11-10");
        assertMappingPresent(info2, "26", "2005-11-09");
        assertMappingPresent(info2, "27", "2005-11-11");
        assertMappingPresent(info2, "28", "2005-11-10");
        assertMappingPresent(info2, "29", "2005-11-10");
        assertEquals(5, info2.getMappings().size());

        GroupMappingInfo groupedInfo  = service.loadGroupedMappings();
        assertGroupingPresent(groupedInfo, "2005-11-10", "25", "28", "29");
        assertGroupingPresent(groupedInfo, "2005-11-09", "26");
        assertGroupingPresent(groupedInfo, "2005-11-11", "27");
        assertEquals(3, groupedInfo.getGroupedMappings().size());

        assertMappedDatePresent();
    }

    @Test
    public void generateUpdateRunTest() throws Exception {
        indexExportSamples();
        GroupMappingOptions options = makeDefaultOptions();
        service.generateMapping(options);

        GroupMappingInfo info = service.loadMappings();
        assertMappingPresent(info, "25", "group1");
        assertMappingPresent(info, "26", "group1");
        assertMappingPresent(info, "27", "group2");
        assertMappingPresent(info, "28", null);
        assertMappingPresent(info, "29", null);
        assertEquals(5, info.getMappings().size());

        options.setUpdate(true);
        options.setGroupField("digitc");

        service.generateMapping(options);

        // Mapping state should have been overwritten
        GroupMappingInfo info2 = service.loadMappings();
        assertMappingPresent(info2, "25", "group1");
        assertMappingPresent(info2, "26", "group1");
        assertMappingPresent(info2, "27", "group2");
        assertMappingPresent(info2, "28", "2005-11-10");
        assertMappingPresent(info2, "29", "2005-11-10");
        assertEquals(5, info2.getMappings().size());

        assertMappedDatePresent();
    }

    @Test
    public void generateUpdateForceRunTest() throws Exception {
        indexExportSamples();
        GroupMappingOptions options = makeDefaultOptions();
        options.setGroupField("digitc");
        service.generateMapping(options);

        GroupMappingInfo info = service.loadMappings();
        assertMappingPresent(info, "25", "2005-11-10");
        assertMappingPresent(info, "26", "2005-11-09");
        assertMappingPresent(info, "27", "2005-11-11");
        assertMappingPresent(info, "28", "2005-11-10");
        assertMappingPresent(info, "29", "2005-11-10");
        assertEquals(5, info.getMappings().size());

        options.setUpdate(true);
        options.setForce(true);
        options.setGroupField("groupa");
        service.generateMapping(options);

        GroupMappingInfo info2 = service.loadMappings();
        assertMappingPresent(info2, "25", "group1");
        assertMappingPresent(info2, "26", "group1");
        assertMappingPresent(info2, "27", "group2");
        assertMappingPresent(info2, "28", "2005-11-10");
        assertMappingPresent(info2, "29", "2005-11-10");
        assertEquals(5, info2.getMappings().size());

        assertMappedDatePresent();
    }

    private GroupMappingOptions makeDefaultOptions() {
        GroupMappingOptions options = new GroupMappingOptions();
        options.setGroupField("groupa");
        return options;
    }

    private void assertExceptionContains(String expected, Exception e) {
        assertTrue("Expected message exception to contain '" + expected + "', but was: " + e.getMessage(),
                e.getMessage().contains(expected));
    }

    private void assertMappingPresent(GroupMappingInfo info, String id, String expectedGroupKey) throws Exception {
        GroupMapping mapping = info.getMappingByCdmId(id);
        assertNotNull(mapping);
        assertEquals(id, mapping.getCdmId());
        assertEquals(expectedGroupKey, mapping.getGroupKey());
    }

    private void assertGroupingPresent(GroupMappingInfo groupedInfo, String groupKey, String... cdmIds) {
        Map<String, List<String>> groupedMappings = groupedInfo.getGroupedMappings();
        List<String> objIds = groupedMappings.get(groupKey);
        List<String> expectedIds = Arrays.asList(cdmIds);
        assertTrue("Expected group " + groupKey + " to contain " + expectedIds + " but contained " + objIds,
                objIds.containsAll(expectedIds));
        assertEquals(expectedIds.size(), objIds.size());
    }

    private void assertMappedDatePresent() throws Exception {
        MigrationProjectProperties props = ProjectPropertiesSerialization.read(project.getProjectPropertiesPath());
        assertNotNull(props.getGroupMappingsUpdatedDate());
    }

    private void assertMappedDateNotPresent() throws Exception {
        MigrationProjectProperties props = ProjectPropertiesSerialization.read(project.getProjectPropertiesPath());
        assertNull(props.getGroupMappingsUpdatedDate());
    }

    private void indexExportSamples() throws Exception {
        Files.copy(Paths.get("src/test/resources/sample_exports/export_1.xml"),
                project.getExportPath().resolve("export_1.xml"));
        Files.copy(Paths.get("src/test/resources/sample_exports/export_2.xml"),
                project.getExportPath().resolve("export_2.xml"));
        Files.copy(Paths.get("src/test/resources/gilmer_fields.csv"), project.getFieldsPath());

        project.getProjectProperties().setExportedDate(Instant.now());
        indexService.createDatabase(true);
        indexService.indexAll();
    }
}
