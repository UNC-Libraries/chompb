package edu.unc.lib.boxc.migration.cdm;

import edu.unc.lib.boxc.common.xml.SecureXMLFactory;
import edu.unc.lib.boxc.migration.cdm.services.DescriptionsService;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static edu.unc.lib.boxc.model.api.xml.JDOMNamespaceUtil.MODS_V3_NS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author bbpennel
 */
public class DescriptionsCommandIT extends AbstractCommandIT {
    private final static Pattern GENERATED_PATH_PATTERN = Pattern.compile(
            ".*Description file generated at: ([^\\s]+).*", Pattern.DOTALL);

    @BeforeEach
    public void setup() throws Exception {
        initProjectAndHelper();
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
        assertOutputContains("Premature end of file");

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
        // Use full set of records
        indexExportSamples("gilmer");

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
        assertOutputMatches(".*Object MODS Records: +3 \\(1.9%\\).*");
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
        assertOutputMatches(".*Object MODS Records: +3 \\(1.9%\\).*");
        assertOutputMatches(".*MODS Files: +1.*");
        assertOutputMatches(".*Objects without MODS: +158\n + \\* 88\n + \\* 89.*");
        assertOutputMatches(".*New Collections MODS: +0.*");
    }

    @Test
    public void validateAllValidModsTest() throws Exception {
        indexExportSamples();

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "descriptions", "generate" };
        executeExpectSuccess(args);

        String[] args3 = new String[] {
                "-w", project.getProjectPath().toString(),
                "descriptions", "validate" };
        executeExpectSuccess(args3);

        assertOutputContains("PASS: All description files are valid");
    }

    @Test
    public void validateInvalidModsTest() throws Exception {
        indexExportSamples();
        XMLOutputter xmlOutputter = new XMLOutputter(Format.getPrettyFormat());

        Document doc = new Document(
                new Element("modsCollection", MODS_V3_NS)
                    .addContent(new Element("mods", MODS_V3_NS)
                        .addContent(new Element("title",MODS_V3_NS)
                                .setText("My title"))));
        xmlOutputter.output(doc, Files.newOutputStream(project.getDescriptionsPath().resolve("25.xml")));

        // Invalid mods for a new collection, wrong root element
        Document doc2 = new Document(
                new Element("modsCollection", MODS_V3_NS)
                    .addContent(new Element("mods", MODS_V3_NS)
                        .addContent(new Element("titleInfo", MODS_V3_NS)
                            .addContent(new Element("title",MODS_V3_NS)
                                .setText("My title")))));
        xmlOutputter.output(doc2, Files.newOutputStream(
                project.getNewCollectionDescriptionsPath().resolve("coll.xml")));

        String[] args3 = new String[] {
                "-w", project.getProjectPath().toString(),
                "descriptions", "validate" };
        executeExpectFailure(args3);

        assertOutputContains("FAIL: Description files are invalid due to the following 2 errors:");
        assertOutputMatches(".*Invalid content was found starting with element" +
                " '\\{\"http://www.loc.gov/mods/v3\":title\\}'.*");
        assertOutputMatches(".*Unexpected root element.* expecting 'mods:mods' but was 'mods:modsCollection'.*");
    }

    private void assertHasModsRecord(List<Element> modsEls, String cdmId) {
        assertTrue(modsEls.stream().anyMatch(modsEl -> cdmId.equals(modsEl.getChildText("identifier", MODS_V3_NS))),
                "No mods record with cdm id " + cdmId);
    }

    private void indexExportSamples() throws Exception {
        indexExportSamples("mini_gilmer");
    }

    private void indexExportSamples(String descPath) throws Exception {
        testHelper.indexExportData(descPath);
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
        assertEquals(expected, fileCount, "Unexpected number of expanded MODS files");
    }

    private void setIndexedDate() throws Exception {
        project.getProjectProperties().setIndexedDate(Instant.now());
        ProjectPropertiesSerialization.write(project);
    }
}
