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

import static edu.unc.lib.boxc.model.api.xml.JDOMNamespaceUtil.MODS_V3_NS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.jdom2.Document;
import org.jdom2.Element;
import org.junit.Before;
import org.junit.Test;

import edu.unc.lib.boxc.common.xml.SecureXMLFactory;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.CdmFieldService;
import edu.unc.lib.boxc.migration.cdm.services.CdmIndexService;
import edu.unc.lib.boxc.migration.cdm.services.DescriptionsService;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;

/**
 * @author bbpennel
 */
public class DescriptionsCommandIT extends AbstractCommandIT {
    private final static Pattern GENERATED_PATH_PATTERN = Pattern.compile(
            ".*Description file generated at: ([^\\s]+).*", Pattern.DOTALL);
    private final static String COLLECTION_ID = "my_coll";

    private MigrationProject project;
    private CdmIndexService indexService;
    private CdmFieldService fieldService;

    @Before
    public void setup() throws Exception {
        project = MigrationProjectFactory.createMigrationProject(
                baseDir, COLLECTION_ID, null, USERNAME);
    }

    @Test
    public void expandDescriptions() throws Exception {
        setIndexedDate();

        Files.copy(Paths.get("src/test/resources/mods_collections/gilmer_mods1.xml"),
                project.getDescriptionsPath().resolve("gilmer_mods1.xml"));

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "descriptions", "expand" };
        executeExpectSuccess(args);
        assertOutputContains("Descriptions expanded to 3 separate files");

        assertExpandedDescriptionFilesCount(3);
    }

    @Test
    public void expandNoDescriptionsFiles() throws Exception {
        setIndexedDate();

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "descriptions", "expand" };
        executeExpectSuccess(args);
        assertOutputContains("Descriptions expanded to 0 separate files");

        assertFalse(Files.exists(project.getExpandedDescriptionsPath()));
    }

    @Test
    public void expandBadInputFile() throws Exception {
        Files.createFile(project.getDescriptionsPath().resolve("input.xml"));

        setIndexedDate();

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "descriptions", "expand" };
        executeExpectFailure(args);
        assertOutputContains("Unexpected EOF");

        assertFalse(Files.exists(project.getExpandedDescriptionsPath()));
    }

    @Test
    public void generateDescriptions() throws Exception {
        indexExportSamples();

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "descriptions", "generate" };
        executeExpectSuccess(args);
        assertOutputContains("Generated 3 dummy descriptions");

        Matcher pathMatcher = GENERATED_PATH_PATTERN.matcher(output);
        assertTrue(pathMatcher.matches());
        Path generatedPath = Paths.get(pathMatcher.group(1));
        assertTrue(Files.exists(generatedPath));
        Document modsCollDoc = SecureXMLFactory.createSAXBuilder().build(generatedPath.toFile());
        List<Element> modsEls = modsCollDoc.getRootElement().getChildren("mods", MODS_V3_NS);
        assertHasModsRecord(modsEls, "25");
        assertHasModsRecord(modsEls, "26");
        assertHasModsRecord(modsEls, "27");
        assertEquals(3, modsEls.size());
    }

    @Test
    public void generateDescriptionsAlreadyExists() throws Exception {
        indexExportSamples();
        DescriptionsService descService = new DescriptionsService();
        descService.setProject(project);

        Files.createFile(descService.getGeneratedModsPath());

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "descriptions", "generate" };
        executeExpectFailure(args);
        assertOutputContains("already exists, use the force flag");

        String[] argsForce = new String[] {
                "-w", project.getProjectPath().toString(),
                "descriptions", "generate",
                "-f" };
        executeExpectSuccess(argsForce);

        Matcher pathMatcher = GENERATED_PATH_PATTERN.matcher(output);
        assertTrue(pathMatcher.matches());
        Path generatedPath = Paths.get(pathMatcher.group(1));
        assertTrue(Files.exists(generatedPath));
        Document modsCollDoc = SecureXMLFactory.createSAXBuilder().build(generatedPath.toFile());
        List<Element> modsEls = modsCollDoc.getRootElement().getChildren("mods", MODS_V3_NS);
        assertHasModsRecord(modsEls, "25");
        assertHasModsRecord(modsEls, "26");
        assertHasModsRecord(modsEls, "27");
        assertEquals(3, modsEls.size());
    }

    @Test
    public void statusAllWithMods() throws Exception {
        indexExportSamples();

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "descriptions", "generate" };
        executeExpectSuccess(args);

        String[] args2 = new String[] {
                "-w", project.getProjectPath().toString(),
                "descriptions", "expand" };
        executeExpectSuccess(args2);
        assertOutputContains("Descriptions expanded to 3 separate files");

        String[] args3 = new String[] {
                "-w", project.getProjectPath().toString(),
                "descriptions", "status" };
        executeExpectSuccess(args3);

        assertOutputMatches(".*Last Expanded: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Object MODS Records: +3 .*");
        assertOutputMatches(".*MODS Files: +1.*");
        assertOutputMatches(".*New Collections MODS: +0.*");
    }

    @Test
    public void statusUnexpanded() throws Exception {
        indexExportSamples();

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "descriptions", "generate" };
        executeExpectSuccess(args);

        String[] args3 = new String[] {
                "-w", project.getProjectPath().toString(),
                "descriptions", "status" };
        executeExpectSuccess(args3);

        assertOutputMatches(".*Last Expanded: +Not completed.*");
        assertOutputMatches(".*Object MODS Records: +3 .*");
        assertOutputMatches(".*MODS Files: +1.*");
        assertOutputMatches(".*New Collections MODS: +0.*");
    }

    @Test
    public void statusMissingMods() throws Exception {
        Files.createDirectories(project.getExportPath());
        // Add second half of records as well
        Files.copy(Paths.get("src/test/resources/sample_exports/export_2.xml"),
                project.getExportPath().resolve("export_2.xml"));
        indexExportSamples();

        // Only adding half of the MODS records
        Files.copy(Paths.get("src/test/resources/mods_collections/gilmer_mods1.xml"),
                project.getDescriptionsPath().resolve("gilmer_mods1.xml"));

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "descriptions", "expand" };
        executeExpectSuccess(args);
        assertOutputContains("Descriptions expanded to 3 separate files");

        resetOutput();

        String[] args2 = new String[] {
                "-w", project.getProjectPath().toString(),
                "descriptions", "status" };
        executeExpectSuccess(args2);

        assertOutputMatches(".*Last Expanded: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Object MODS Records: +3 \\(60.0%\\).*");
        assertOutputMatches(".*MODS Files: +1.*");
        assertOutputNotMatches(".*Objects without MODS.*");
        assertOutputMatches(".*New Collections MODS: +0.*");

        resetOutput();

        String[] args3 = new String[] {
                "-w", project.getProjectPath().toString(),
                "descriptions", "status",
                "-v" };
        executeExpectSuccess(args3);

        assertOutputMatches(".*Last Expanded: +[0-9\\-T:]+.*");
        assertOutputMatches(".*Object MODS Records: +3 \\(60.0%\\).*");
        assertOutputMatches(".*MODS Files: +1.*");
        assertOutputMatches(".*Objects without MODS: +2\n + \\* 28\n + \\* 29.*");
        assertOutputMatches(".*New Collections MODS: +0.*");
    }

    private void assertHasModsRecord(List<Element> modsEls, String cdmId) {
        assertTrue("No mods record with cdm id " + cdmId,
                modsEls.stream().anyMatch(modsEl -> cdmId.equals(modsEl.getChildText("identifier", MODS_V3_NS))));
    }

    private void indexExportSamples() throws Exception {
        fieldService = new CdmFieldService();
        indexService = new CdmIndexService();
        indexService.setFieldService(fieldService);
        indexService.setProject(project);

        Files.createDirectories(project.getDescriptionsPath());
        Files.createDirectories(project.getExportPath());
        Files.copy(Paths.get("src/test/resources/sample_exports/export_1.xml"),
                project.getExportPath().resolve("export_all.xml"));
        Files.copy(Paths.get("src/test/resources/gilmer_fields.csv"), project.getFieldsPath());

        project.getProjectProperties().setExportedDate(Instant.now());
        indexService.createDatabase(true);
        indexService.indexAll();
    }

    private void assertExpandedDescriptionFilesCount(int expected) throws Exception {
        int fileCount = 0;
        try (DirectoryStream<Path> nested = Files.newDirectoryStream(project.getExpandedDescriptionsPath())) {
            for (Path nestedDir : nested) {
                try (Stream<Path> files = Files.list(nestedDir)) {
                    fileCount += files.count();
                }
            }
        }
        assertEquals("Unexpected number of expanded MODS files", expected, fileCount);
    }

    private void setIndexedDate() throws Exception {
        project.getProjectProperties().setIndexedDate(Instant.now());
        ProjectPropertiesSerialization.write(project);
    }
}
