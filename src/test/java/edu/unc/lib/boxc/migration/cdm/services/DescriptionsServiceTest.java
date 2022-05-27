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

import static edu.unc.lib.boxc.model.api.xml.JDOMNamespaceUtil.MODS_V3_NS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import org.apache.commons.io.FileUtils;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.output.XMLOutputter;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import edu.unc.lib.boxc.common.xml.SecureXMLFactory;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProjectProperties;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;

/**
 * @author bbpennel
 */
public class DescriptionsServiceTest {
    private static final String PROJECT_NAME = "proj";

    @Rule
    public final TemporaryFolder tmpFolder = new TemporaryFolder();
    private final static XMLOutputter xmlOutputter = new XMLOutputter();

    private MigrationProject project;
    private DescriptionsService service;

    private Path basePath;

    @Before
    public void setup() throws Exception {
        basePath = tmpFolder.newFolder().toPath();
        project = MigrationProjectFactory.createMigrationProject(basePath, PROJECT_NAME, null, "user", CdmEnvironmentHelper.DEFAULT_ENV);
        Files.createDirectories(project.getDescriptionsPath());
        service = new DescriptionsService();
        service.setProject(project);
    }

    @Test
    public void expandCollectionsOneFile() throws Exception {
        Files.copy(Paths.get("src/test/resources/mods_collections/gilmer_mods1.xml"),
                project.getDescriptionsPath().resolve("gilmer_mods1.xml"));
        Set<String> idsWithMods = service.expandDescriptions();
        assertEquals(3, idsWithMods.size());
        assertDatePresent();

        assertTrue(Files.exists(project.getExpandedDescriptionsPath()));
        assertModsPopulated("Redoubt C", "25");
        assertModsPopulated("Plan of Battery McIntosh", "26");
        assertModsPopulated("Fort DeRussy on Red River, Louisiana", "27");
        assertExpandedDescriptionFilesCount(3);
    }

    @Test
    public void expandCollectionsMultipleFiles() throws Exception {
        Files.copy(Paths.get("src/test/resources/mods_collections/gilmer_mods1.xml"),
                project.getDescriptionsPath().resolve("gilmer_mods1.xml"));
        Files.copy(Paths.get("src/test/resources/mods_collections/gilmer_mods2.xml"),
                project.getDescriptionsPath().resolve("gilmer_mods2.xml"));
        Set<String> idsWithMods = service.expandDescriptions();
        assertEquals(6, idsWithMods.size());
        assertDatePresent();

        assertTrue(Files.exists(project.getExpandedDescriptionsPath()));
        assertModsPopulated("Redoubt C", "25");
        assertModsPopulated("Plan of Battery McIntosh", "26");
        assertModsPopulated("Fort DeRussy on Red River, Louisiana", "27");
        assertModsPopulated("Map of the country adjacent to Smithville", "28");
        // Only the first cdm id from the mods record is used
        assertModsPopulated("Fort Fisher and adjoining fortifications", "29");
        assertModsPopulated("Unmapped", "555");
        assertExpandedDescriptionFilesCount(6);
    }

    @Test
    public void expandBadRootElement() throws Exception {
        Document doc = new Document();
        doc.setRootElement(new Element("badRoot"));
        xmlOutputter.output(doc, Files.newOutputStream(project.getDescriptionsPath().resolve("input.xml")));

        try {
            service.expandDescriptions();
            fail();
        } catch (MigrationException e) {
            assertTrue(e.getMessage().contains("Root element is not a mods:collection"));
        }
        assertFalse(Files.exists(project.getExpandedDescriptionsPath()));
        assertDateNotPresent();
    }

    @Test
    public void expandBadChildElement() throws Exception {
        Document doc = new Document();
        doc.setRootElement(new Element("modsCollection", MODS_V3_NS)
                .addContent(new Element("titleInfo", MODS_V3_NS).setText("i'm confused")));
        xmlOutputter.output(doc, Files.newOutputStream(project.getDescriptionsPath().resolve("input.xml")));

        try {
            service.expandDescriptions();
            fail();
        } catch (MigrationException e) {
            assertTrue(e.getMessage().contains("Children of mods:collection must be mods:mods"));
        }
        assertFalse(Files.exists(project.getExpandedDescriptionsPath()));
        assertDateNotPresent();
    }

    @Test
    public void expandInvalidDocument() throws Exception {
        FileUtils.writeStringToFile(project.getDescriptionsPath().resolve("input.xml").toFile(),
                "<mods:modsCollection xmlns:mods=\"http://www.loc.gov/mods/v3\">"
                + "<mods:mods><mods:titleInfo><mods:title>stuff</mods:titleInfo>"
                + "</mods:mods>"
                + "</mods:modsCollection>",
                StandardCharsets.UTF_8);

        try {
            service.expandDescriptions();
            fail();
        } catch (MigrationException e) {
            assertTrue("Unexpected message: " + e.getMessage(), e.getMessage().contains("Unexpected close tag"));
        }
        assertFalse(Files.exists(project.getExpandedDescriptionsPath()));
        assertDateNotPresent();
    }

    @Test
    public void expandEmptyCollectionDocument() throws Exception {
        FileUtils.writeStringToFile(project.getDescriptionsPath().resolve("input.xml").toFile(),
                "<mods:modsCollection xmlns:mods=\"http://www.loc.gov/mods/v3\">"
                + "</mods:modsCollection>",
                StandardCharsets.UTF_8);

        service.expandDescriptions();
        assertFalse(Files.exists(project.getExpandedDescriptionsPath()));
        assertDateNotPresent();
    }

    @Test
    public void expandEmptyDocument() throws Exception {
        Files.createFile(project.getDescriptionsPath().resolve("input.xml"));

        try {
            service.expandDescriptions();
            fail();
        } catch (MigrationException e) {
            assertTrue("Unexpected message: " + e.getMessage(), e.getMessage().contains("Unexpected EOF"));
        }
        assertFalse(Files.exists(project.getExpandedDescriptionsPath()));
        assertDateNotPresent();
    }

    @Test
    public void expandRunTwice() throws Exception {
        Files.copy(Paths.get("src/test/resources/mods_collections/gilmer_mods1.xml"),
                project.getDescriptionsPath().resolve("gilmer_mods1.xml"));
        service.expandDescriptions();

        assertTrue(Files.exists(project.getExpandedDescriptionsPath()));
        Files.delete(service.getExpandedDescriptionFilePath("26"));
        assertExpandedDescriptionFilesCount(2);

        // Expect deleted record to regenerate, and no errors from duplicates
        Set<String> idsWithMods = service.expandDescriptions();
        assertEquals(3, idsWithMods.size());
        assertModsPopulated("Redoubt C", "25");
        assertModsPopulated("Plan of Battery McIntosh", "26");
        assertModsPopulated("Fort DeRussy on Red River, Louisiana", "27");
        assertExpandedDescriptionFilesCount(3);
        assertDatePresent();
    }

    private void assertModsPopulated(String expectedTitle, String expectedId) throws Exception {
        Path path = service.getExpandedDescriptionFilePath(expectedId);
        Document modsDoc = SecureXMLFactory.createSAXBuilder().build(path.toFile());
        Element rootEl = modsDoc.getRootElement();
        assertEquals("mods", rootEl.getName());
        assertEquals(MODS_V3_NS, rootEl.getNamespace());
        assertEquals(expectedTitle, rootEl.getChild("titleInfo", MODS_V3_NS).getChildText("title", MODS_V3_NS));

        Optional<Element> idEl = rootEl.getChildren("identifier", MODS_V3_NS).stream()
            .filter(e -> "local".equals(e.getAttributeValue("type")))
            .filter(e -> "CONTENTdm number".equals(e.getAttributeValue("displayLabel")))
            .findFirst();
        assertTrue("Expected to find CDM number identifier field", idEl.isPresent());
        assertEquals(expectedId, idEl.get().getText());
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

    private void assertDatePresent() throws Exception {
        MigrationProjectProperties props = ProjectPropertiesSerialization.read(project.getProjectPropertiesPath());
        assertNotNull(props.getDescriptionsExpandedDate());
    }

    private void assertDateNotPresent() throws Exception {
        MigrationProjectProperties props = ProjectPropertiesSerialization.read(project.getProjectPropertiesPath());
        assertNull(props.getDescriptionsExpandedDate());
    }
}
