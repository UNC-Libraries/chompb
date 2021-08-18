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
package edu.unc.lib.boxc.migration.cdm;

import static org.junit.Assert.assertEquals;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import edu.unc.lib.boxc.migration.cdm.model.DestinationsInfo;
import edu.unc.lib.boxc.migration.cdm.model.DestinationsInfo.DestinationMapping;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.DestinationsService;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.test.SipServiceHelper;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;

/**
 * @author bbpennel
 */
public class DestinationsCommandIT extends AbstractCommandIT {
    private final static String COLLECTION_ID = "my_coll";
    private final static String DEST_UUID = "3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e";

    private MigrationProject project;
    private SipServiceHelper testHelper;

    @Before
    public void setup() throws Exception {
        project = MigrationProjectFactory.createMigrationProject(
                baseDir, COLLECTION_ID, null, USERNAME);
        testHelper = new SipServiceHelper(project, tmpFolder.getRoot().toPath());
    }

    @Test
    public void generateNotIndexedTest() throws Exception {
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "destinations", "generate",
                "-dd", DEST_UUID};
        executeExpectFailure(args);

        assertOutputContains("Project must be indexed");
    }

    @Test
    public void generateWithNoOptionsTest() throws Exception {
        setIndexedDate();

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "destinations", "generate"};
        executeExpectFailure(args);
        assertOutputContains("Must provide a default destination");
    }

    @Test
    public void generateWithDefaultDestTest() throws Exception {
        setIndexedDate();

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "destinations", "generate",
                "-dd", DEST_UUID};
        executeExpectSuccess(args);

        assertDefaultMapping(DEST_UUID, "");
    }

    @Test
    public void generateWithDefaultDestAndCollTest() throws Exception {
        setIndexedDate();

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "destinations", "generate",
                "-dd", DEST_UUID,
                "-dc", "00123" };
        executeExpectSuccess(args);

        assertDefaultMapping(DEST_UUID, "00123");
    }

    @Test
    public void generateWithInvalidDefaultDestTest() throws Exception {
        setIndexedDate();

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "destinations", "generate",
                "-dd", "nothanks"};
        executeExpectFailure(args);
        assertOutputContains("must be a valid UUID");
    }

    @Test
    public void generateMappingExistsTest() throws Exception {
        setIndexedDate();

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "destinations", "generate",
                "-dd", DEST_UUID};
        executeExpectSuccess(args);
        assertDefaultMapping(DEST_UUID, "");

        // Should fail the second time and not update anything
        String[] args2 = new String[] {
                "-w", project.getProjectPath().toString(),
                "destinations", "generate",
                "-dd", DEST_UUID,
                "-dc", "00abc"};
        executeExpectFailure(args2);
        assertDefaultMapping(DEST_UUID, "");

        String[] args3 = new String[] {
                "-w", project.getProjectPath().toString(),
                "destinations", "generate",
                "-dd", DEST_UUID,
                "-dc", "00abc",
                "--force"};
        executeExpectSuccess(args3);
        assertDefaultMapping(DEST_UUID, "00abc");
    }

    @Test
    public void validateValidTest() throws Exception {
        setIndexedDate();

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "destinations", "generate",
                "-dd", DEST_UUID,
                "-dc", "00123" };
        executeExpectSuccess(args);

        String[] args2 = new String[] {
                "-w", project.getProjectPath().toString(),
                "destinations", "validate" };
        executeExpectSuccess(args2);

        assertOutputContains("PASS: Destination mapping at path " + project.getDestinationMappingsPath() + " is valid");
    }

    @Test
    public void validateInvalidTest() throws Exception {
        setIndexedDate();

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "destinations", "generate",
                "-dd", DEST_UUID,
                "-dc", "00123" };
        executeExpectSuccess(args);

        // Add a duplicate destination mapping
        FileUtils.write(project.getDestinationMappingsPath().toFile(),
                "25," + DEST_UUID + ",", StandardCharsets.UTF_8, true);

        String[] args2 = new String[] {
                "-w", project.getProjectPath().toString(),
                "destinations", "validate" };
        executeExpectFailure(args2);

        assertOutputContains("FAIL: Destination mapping at path " + project.getDestinationMappingsPath()
                + " is invalid");
        assertOutputContains("- Destination at line 3 has been previously mapped with a new collection");
        assertEquals("Must only be two errors: " + output, 2, output.split("    - ").length);
    }

    @Test
    public void statusValidTest() throws Exception {
        testHelper.indexExportData("export_1.xml");

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "destinations", "generate",
                "-dd", DEST_UUID,
                "-dc", "00123" };
        executeExpectSuccess(args);

        String[] args2 = new String[] {
                "-w", project.getProjectPath().toString(),
                "destinations", "status" };
        executeExpectSuccess(args2);

        assertOutputMatches(".*Last Generated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Objects Mapped: +3 \\(100.0%\\).*");
        assertOutputMatches(".*Unmapped Objects: +0.*");
        assertOutputMatches(".*Destinations Valid: +Yes\n.*");
        assertOutputMatches(".*To Default: +3 \\(100.0%\\).*");
        assertOutputMatches(".*Destinations: +1\n.*");
        assertOutputMatches(".*Destinations:.*\n +\\* " + DEST_UUID + " 00123.*");
        assertOutputMatches(".*New Collections: +1\n.*");
        assertOutputMatches(".*New Collections:.*\n +\\* 00123.*");
    }

    @Test
    public void statusValidQuietTest() throws Exception {
        testHelper.indexExportData("export_1.xml");

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "destinations", "generate",
                "-dd", DEST_UUID,
                "-dc", "00123" };
        executeExpectSuccess(args);

        String[] args2 = new String[] {
                "-w", project.getProjectPath().toString(),
                "destinations", "status",
                "-q" };
        executeExpectSuccess(args2);

        assertOutputMatches(".*Last Generated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Objects Mapped: +3 \\(100.0%\\).*");
        assertOutputNotMatches(".*Unmapped Objects: +0.*");
        assertOutputMatches(".*Destinations Valid: +Yes\n.*");
        assertOutputNotMatches(".*To Default: +3 \\(100.0%\\).*");
        assertOutputMatches(".*Destinations: +1\n.*");
        assertOutputNotMatches(".*\\* " + DEST_UUID + "\\|00123.*");
        assertOutputNotMatches(".*New Collections: +1\n.*");
        assertOutputNotMatches(".*\\* 00123.*");
    }

    @Test
    public void statusValidVerboseTest() throws Exception {
        testHelper.indexExportData("export_1.xml");

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "destinations", "generate",
                "-dd", DEST_UUID,
                "-dc", "00123" };
        executeExpectSuccess(args);

        String[] args2 = new String[] {
                "-w", project.getProjectPath().toString(),
                "destinations", "status",
                "-v" };
        executeExpectSuccess(args2);

        assertOutputMatches(".*Last Generated: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Objects Mapped: +3 \\(100.0%\\).*");
        assertOutputMatches(".*Unmapped Objects: +0 \\(0.0%\\).*");
        assertOutputMatches(".*Destinations Valid: +Yes\n.*");
        assertOutputMatches(".*To Default: +3 \\(100.0%\\).*");
        assertOutputMatches(".*Destinations: +1\n.*");
        assertOutputMatches(".*Destinations:.*\n +\\* " + DEST_UUID + " 00123.*");
        assertOutputMatches(".*New Collections: +1\n.*");
        assertOutputMatches(".*New Collections:.*\n +\\* 00123.*");
    }

    private void assertDefaultMapping(String expectedDest, String expectedColl) throws Exception {
        DestinationsInfo info = DestinationsService.loadMappings(project);
        List<DestinationMapping> mappings = info.getMappings();
        assertEquals(1, mappings.size());
        DestinationMapping mapping = mappings.get(0);
        assertEquals(DestinationsInfo.DEFAULT_ID, mapping.getId());
        assertEquals(expectedDest, mapping.getDestination());
        assertEquals(expectedColl, mapping.getCollectionId());
    }

    private void setIndexedDate() throws Exception {
        project.getProjectProperties().setIndexedDate(Instant.now());
        ProjectPropertiesSerialization.write(project);
    }
}
