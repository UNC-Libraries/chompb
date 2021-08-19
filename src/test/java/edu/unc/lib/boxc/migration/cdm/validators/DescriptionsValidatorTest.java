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
package edu.unc.lib.boxc.migration.cdm.validators;

import static edu.unc.lib.boxc.model.api.xml.JDOMNamespaceUtil.MODS_V3_NS;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;

/**
 * @author bbpennel
 */
public class DescriptionsValidatorTest {
    private static final String PROJECT_NAME = "proj";
    private static final String USERNAME = "migr_user";
    @Rule
    public final TemporaryFolder tmpFolder = new TemporaryFolder();
    private MigrationProject project;
    private DescriptionsValidator validator;
    private static XMLOutputter xmlOutputter = new XMLOutputter(Format.getPrettyFormat());

    @Before
    public void setup() throws Exception {
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder.newFolder().toPath(), PROJECT_NAME, null, USERNAME);

        validator = new DescriptionsValidator();
        validator.setProject(project);
        validator.init();
    }

    @Test
    public void noDescriptionFilesTest() throws Exception {
        List<String> errors = validator.validate();
        assertErrorCount(errors, 0);
    }

    @Test
    public void withValidObjectModsTest() throws Exception {
        Files.copy(Paths.get("src/test/resources/mods_collections/gilmer_mods1.xml"),
                project.getDescriptionsPath().resolve("gilmer_mods1.xml"));
        List<String> errors =  validator.validate();
        assertErrorCount(errors, 0);
    }

    @Test
    public void withInvalidObjectModsTest() throws Exception {
        Element modsColl = new Element("modsCollection", MODS_V3_NS)
                .addContent(new Element("mods", MODS_V3_NS)
                        .addContent(new Element("title",MODS_V3_NS)
                                .setText("My title")));
        Document doc = new Document(modsColl);
        xmlOutputter.output(doc, Files.newOutputStream(project.getDescriptionsPath().resolve("mods.xml")));

        List<String> errors =  validator.validate();
        assertHasErrorMatching(errors, ".*Invalid content .* element 'mods:title'.*");
        assertErrorCount(errors, 1);
    }

    @Test
    public void withObjectModsWrongRootTest() throws Exception {
        Element rootEl = new Element("mods", MODS_V3_NS)
                    .addContent(new Element("titleInfo", MODS_V3_NS)
                        .addContent(new Element("title",MODS_V3_NS)
                                .setText("My title")));
        Document doc = new Document(rootEl);
        xmlOutputter.output(doc, Files.newOutputStream(project.getDescriptionsPath().resolve("mods.xml")));

        List<String> errors =  validator.validate();
        assertHasErrorMatching(errors, "Unexpected root element.* expecting 'mods:modsCollection' but was 'mods:mods'");
        assertErrorCount(errors, 1);
    }

    @Test
    public void withValidCollModsTest() throws Exception {
        Element rootEl = new Element("mods", MODS_V3_NS)
                .addContent(new Element("titleInfo", MODS_V3_NS)
                    .addContent(new Element("title",MODS_V3_NS)
                            .setText("My title")));
        Document doc = new Document(rootEl);
        xmlOutputter.output(doc, Files.newOutputStream(project.getNewCollectionDescriptionsPath().resolve("mods.xml")));
        List<String> errors =  validator.validate();
        assertErrorCount(errors, 0);
    }

    @Test
    public void withInvalidCollModsTest() throws Exception {
        Element rootEl = new Element("mods", MODS_V3_NS)
                    .addContent(new Element("title",MODS_V3_NS)
                        .setText("My title"));
        Document doc = new Document(rootEl);
        xmlOutputter.output(doc, Files.newOutputStream(project.getNewCollectionDescriptionsPath().resolve("mods.xml")));

        List<String> errors =  validator.validate();
        assertHasErrorMatching(errors, ".*Invalid content .* element 'mods:title'.*");
        assertErrorCount(errors, 1);
    }

    @Test
    public void withCollWrongRootTest() throws Exception {
        Element rootEl = new Element("modsCollection", MODS_V3_NS)
                .addContent(new Element("mods", MODS_V3_NS)
                    .addContent(new Element("titleInfo", MODS_V3_NS)
                        .addContent(new Element("title",MODS_V3_NS)
                                .setText("My title"))));
        Document doc = new Document(rootEl);
        xmlOutputter.output(doc, Files.newOutputStream(project.getNewCollectionDescriptionsPath().resolve("mods.xml")));

        List<String> errors =  validator.validate();
        assertHasErrorMatching(errors, "Unexpected root element.* expecting 'mods:mods' but was 'mods:modsCollection'");
        assertErrorCount(errors, 1);
    }

    @Test
    public void objectModsNotXmlTest() throws Exception {
        FileUtils.write(project.getDescriptionsPath().resolve("mods.xml").toFile(), "uh no thanks", UTF_8);

        List<String> errors =  validator.validate();
        assertHasErrorMatching(errors, "File descriptions/mods.xml is not a valid XML.*");
        assertErrorCount(errors, 1);
    }

    private void assertHasErrorMatching(List<String> errors, String expectedP) {
        Pattern pattern = Pattern.compile(expectedP, Pattern.DOTALL);
        assertTrue("Expected error:\n" + expectedP + "\nBut the returned errors were:\n" + String.join("\n", errors),
                errors.stream().anyMatch(e -> pattern.matcher(e).matches()));
    }

    private void assertErrorCount(List<String> errors, int expected) {
        assertEquals("Unexpected number of errors:\n" + String.join("\n", errors) + "\n",
                expected, errors.size());
    }
}
