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
import static org.slf4j.LoggerFactory.getLogger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.XMLConstants;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.slf4j.Logger;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.xml.sax.SAXException;

import edu.unc.lib.boxc.common.xml.SecureXMLFactory;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.operations.api.exceptions.MetadataValidationException;
import edu.unc.lib.boxc.operations.impl.validation.MODSValidator;
import edu.unc.lib.boxc.operations.impl.validation.SchematronValidator;

/**
 * Validate description files against schema and schematron
 *
 * @author bbpennel
 */
public class DescriptionsValidator {
    private static final Logger log = getLogger(DescriptionsValidator.class);
    private MODSValidator modsValidator;
    private MigrationProject project;

    /**
     * Validate the object and new collection descriptions for this project
     * @return List of errors, or empty list if all valid
     */
    public List<String> validate() {
        List<String> errors = new ArrayList<>();
        validateFilesInPath(project.getDescriptionsPath(), "modsCollection", errors);
        validateFilesInPath(project.getNewCollectionDescriptionsPath(), "mods", errors);
        return errors;
    }

    private void validateFilesInPath(Path dirPath, String expectedRootElName, List<String> errors) {
        SAXBuilder builder = SecureXMLFactory.createSAXBuilder();
        try (DirectoryStream<Path> pathStream = Files.newDirectoryStream(dirPath, "*.xml")) {
            for (Path xmlPath : pathStream) {
                Path relative = project.getProjectPath().relativize(xmlPath);
                try {
                    log.info("Validating file: {}", xmlPath);
                    byte[] bytes = Files.readAllBytes(xmlPath);
                    ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
                    Document doc = builder.build(byteStream);
                    Element rootEl = doc.getRootElement();
                    if (!(rootEl.getName().equals(expectedRootElName) && rootEl.getNamespace().equals(MODS_V3_NS))) {
                        errors.add("Unexpected root element in document " + relative + ", expecting '"
                            + new Element(expectedRootElName, MODS_V3_NS).getQualifiedName()
                            + "' but was '" + rootEl.getQualifiedName() + "'");
                    }
                    byteStream.reset();
                    modsValidator.validate(byteStream);
                } catch (MetadataValidationException e) {
                    errors.add("File " + relative + " did not pass validation:\n    " +
                            (e.getDetailedMessage() == null ? e.getMessage() : e.getDetailedMessage()));
                } catch (JDOMException e) {
                    log.warn("Failed to parse {}", xmlPath, e);
                    errors.add("File " + relative + " is not a valid XML document: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            log.error("Failed to validate", e);
            errors.add("Failed to validate due to error: " + e.getMessage());
        }
    }

    public void init() {
        Map<String, Resource> schemas = new HashMap<>();
        schemas.put("object-mods", new ClassPathResource(
                "edu/unc/lib/boxc/operations/impl/validation/object-mods.sch"));
        schemas.put("vocabularies-mods", new ClassPathResource(
                "edu/unc/lib/boxc/operations/impl/validation/vocabularies-mods.sch"));

        SchematronValidator schematronValidator = new SchematronValidator();
        schematronValidator.setSchemas(schemas);
        schematronValidator.loadSchemas();

        StreamSource[] xsdSources = {
                new StreamSource(getClass().getResourceAsStream("/schemas/xml.xsd")),
                new StreamSource(getClass().getResourceAsStream("/schemas/xlink.xsd")),
                new StreamSource(getClass().getResourceAsStream("/schemas/mods-3-7.xsd"))
        };

        SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        Schema schema;
        try {
            schema = factory.newSchema(xsdSources);
        } catch (SAXException e) {
            throw new MigrationException(e);
        }

        modsValidator = new MODSValidator();
        modsValidator.setModsSchema(schema);
        modsValidator.setSchematronValidator(schematronValidator);
    }

    public void setProject(MigrationProject project) {
        this.project = project;
    }
}
