package edu.unc.lib.boxc.migration.cdm.validators;

import static edu.unc.lib.boxc.model.api.xml.JDOMNamespaceUtil.MODS_V3_NS;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.regex.Pattern;

import edu.unc.lib.boxc.migration.cdm.test.BxcEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import org.apache.commons.io.FileUtils;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.MigrationProjectFactory;

/**
 * @author bbpennel
 */
public class DescriptionsValidatorTest {
    private static final String PROJECT_NAME = "proj";
    private static final String USERNAME = "migr_user";
    @TempDir
    public Path tmpFolder;
    private MigrationProject project;
    private DescriptionsValidator validator;
    private static XMLOutputter xmlOutputter = new XMLOutputter(Format.getPrettyFormat());

    @BeforeEach
    public void setup() throws Exception {
        project = MigrationProjectFactory.createCdmMigrationProject(
                tmpFolder, PROJECT_NAME, null, USERNAME,
                CdmEnvironmentHelper.DEFAULT_ENV_ID, BxcEnvironmentHelper.DEFAULT_ENV_ID);

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
        assertHasErrorMatching(errors, ".*Invalid content .* element '\\{\"http://www.loc.gov/mods/v3\":title.*");
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
        assertHasErrorMatching(errors, ".*Invalid content .* element '\\{\"http://www.loc.gov/mods/v3\":title\\}'.*");
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
        assertTrue(errors.stream().anyMatch(e -> pattern.matcher(e).matches()),
                "Expected error:\n" + expectedP + "\nBut the returned errors were:\n" + String.join("\n", errors));
    }

    private void assertErrorCount(List<String> errors, int expected) {
        assertEquals(expected, errors.size(),
                "Unexpected number of errors:\n" + String.join("\n", errors) + "\n");
    }
}
