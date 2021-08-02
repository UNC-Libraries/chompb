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

import static edu.unc.lib.boxc.common.xml.SecureXMLFactory.createXMLInputFactory;
import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;
import static edu.unc.lib.boxc.model.api.xml.JDOMNamespaceUtil.MODS_V3_NS;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.Characters;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.lib.boxc.common.xml.SecureXMLFactory;
import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;

/**
 * Service for interacting with MODS description files
 *
 * @author bbpennel
 */
public class DescriptionsService {
    private static final Logger log = LoggerFactory.getLogger(DescriptionsService.class);
    private static final int EXPANDED_FILES_PER_DIR = 1024;

    private static final QName TYPE_NAME = new QName("type");
    private static final QName DISPLAY_LABEL_NAME = new QName("displayLabel");
    private static final QName IDENTIFIER_NAME = new QName(MODS_V3_NS.getURI(), "identifier");
    private static final QName COLLECTION_NAME = new QName(MODS_V3_NS.getURI(), "modsCollection");
    private static final QName MODS_NAME = new QName(MODS_V3_NS.getURI(), "mods");

    public static final String CDM_NUMBER_LABEL = "CONTENTdm number";
    private static final String LOCAL_TYPE_VALUE = "local";

    public static final String GENERATED_MODS_FILENAME = "generated_mods.xml";


    private MigrationProject project;
    private final XMLOutputFactory xmlOutput = XMLOutputFactory.newInstance();
    private final SAXBuilder xmlBuilder = SecureXMLFactory.createSAXBuilder();
    private final XMLOutputter xmlOutputter = new XMLOutputter(Format.getPrettyFormat());

    public DescriptionsService() {
    }

    /**
     * Expand the modsCollection files located in the descriptions folder out into individual MODS records by cdm id
     * @throws IOException
     */
    public int expandDescriptions() throws IOException {
        int recordsExtracted = 0;
        try (DirectoryStream<Path> pathStream = Files.newDirectoryStream(project.getDescriptionsPath(), "*.xml")) {
            for (Path path : pathStream) {
                recordsExtracted += expandModsCollectionFile(path);
            }
        }
        if (recordsExtracted > 0) {
            project.getProjectProperties().setDescriptionsExpandedDate(Instant.now());
            ProjectPropertiesSerialization.write(project);
        }
        return recordsExtracted;
    }

    private int expandModsCollectionFile(Path collFile) {
        // Enable so that namespace properties will be added to the split out MODS documents if needed
        xmlOutput.setProperty(XMLOutputFactory.IS_REPAIRING_NAMESPACES, Boolean.TRUE);
        int recordsExtracted = 0;

        try (InputStream xmlStream = Files.newInputStream(collFile)) {
            XMLEventReader xmlReader = createXMLInputFactory().createXMLEventReader(xmlStream);
            StringWriter modsWriter = null;
            XMLEventWriter xmlWriter = null;
            boolean inCollection = false;
            boolean inMods = false;
            boolean inCdmIdentifier = false;
            int openTags = 0;
            String cdmId = null;

            outputLogger.info("Expanding description file {}", collFile);
            log.debug("Beginning expansion of MODS collection file {}", collFile);
            while (xmlReader.hasNext()) {
                XMLEvent e = xmlReader.nextEvent();

                if (!inCollection) {
                    if (e.isStartElement()) {
                        StartElement el = e.asStartElement();
                        // Make sure that this document begins with bulk md tag
                        if (el.getName().equals(COLLECTION_NAME)) {
                            log.debug("Starting MODS collection");
                            inCollection = true;
                        } else {
                            throw new MigrationException("Root element is not a mods:collection, is a " + el.getName());
                        }
                    }
                } else if (!inMods) {
                    if (e.isStartElement()) {
                        StartElement el = e.asStartElement();
                        // Make sure that this document begins with bulk md tag
                        if (el.getName().equals(MODS_NAME)) {
                            log.debug("Starting MODS record");
                            inMods = true;
                            openTags = 1;
                            cdmId = null;
                            modsWriter = new StringWriter();
                            xmlWriter = xmlOutput.createXMLEventWriter(modsWriter);
                            xmlWriter.add(e);
                        } else {
                            throw new MigrationException("Children of mods:collection must be mods:mods, but found "
                                    + el.getName());
                        }
                    } else if (e.isEndElement()) {
                        log.debug("Finished processing MODS collection");
                        break;
                    }
                } else {
                    xmlWriter.add(e);
                    if (inCdmIdentifier && e.isCharacters()) {
                        Characters chars = e.asCharacters();
                        cdmId = chars.getData();
                        log.debug("Found cdmid {}", cdmId);
                        inCdmIdentifier = false;
                    } else if (e.isStartElement()) {
                        StartElement el = e.asStartElement();
                        // Track number of tags open so we can tell when the mods element ends
                        openTags++;

                        if (cdmId == null && isCdmIdentifier(el)) {
                            inCdmIdentifier = true;
                        }
                    } else if (e.isEndElement()) {
                        openTags--;
                    }
                    // Closing of the MODS element, time to write
                    if (openTags == 0) {
                        inMods = false;
                        xmlWriter.close();
                        xmlWriter = null;

                        // Write the MODS record to a file
                        if (cdmId != null) {
                            Path descPath = getExpandedDescriptionFilePath(cdmId);
                            // Make sure the directory exists and overwrite any existing file
                            Files.createDirectories(descPath.getParent());
                            if (Files.deleteIfExists(descPath)) {
                                log.debug("Overwriting existing MODS file {}", descPath);
                            }
                            // Pass through xmlOutputter to fix indentation issues and add xml declaration
                            Document doc = xmlBuilder.build(new ByteArrayInputStream(modsWriter.toString().getBytes()));
                            xmlOutputter.output(doc, Files.newOutputStream(descPath));

                            recordsExtracted++;
                        } else {
                            log.warn("MODS record does not contain a CDM ID, skipping: {}", modsWriter);
                        }
                    }
                }
            }
        } catch (IOException | XMLStreamException | JDOMException e) {
            throw new MigrationException(e.getMessage(), e);
        }
        return recordsExtracted;
    }

    private boolean isCdmIdentifier(StartElement element) {
        if (!element.getName().equals(IDENTIFIER_NAME)) {
            return false;
        }
        Attribute typeAttr = element.getAttributeByName(TYPE_NAME);
        if (typeAttr == null || !LOCAL_TYPE_VALUE.equals(typeAttr.getValue())) {
            return false;
        }
        Attribute labelAttr = element.getAttributeByName(DISPLAY_LABEL_NAME);
        if (labelAttr == null || !CDM_NUMBER_LABEL.equals(labelAttr.getValue())) {
            return false;
        }
        return true;
    }

    /**
     * @param cdmId
     * @return The path for the individual MODS file
     */
    public Path getExpandedDescriptionFilePath(String cdmId) {
        int cdmIdNum = Integer.parseInt(cdmId);
        String subdir = Integer.toString(cdmIdNum / EXPANDED_FILES_PER_DIR);
        return project.getExpandedDescriptionsPath().resolve(subdir).resolve(cdmId + ".xml");
    }

    /**
     * Generates a modsCollection document with dummy records for all objects in this project
     * @return
     */
    public int generateDocuments(boolean force) {
        CdmIndexService indexService = new CdmIndexService();
        indexService.setProject(project);

        int cnt = 0;
        Connection conn = null;
        try {
            Path path = project.getDescriptionsPath().resolve(GENERATED_MODS_FILENAME);
            if (Files.exists(path)) {
                if (force) {
                    Files.delete(path);
                } else {
                    throw new InvalidProjectStateException(GENERATED_MODS_FILENAME + " already exists, "
                            + "use the force flag to overwrite.");
                }
            }

            conn = indexService.openDbConnection();
            Document doc = new Document();
            Element collEl = new Element("modsCollection", MODS_V3_NS);
            doc.setRootElement(collEl);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select " + CdmFieldInfo.CDM_ID + " from " + CdmIndexService.TB_NAME);
            while (rs.next()) {
                String cdmId = rs.getString(1);
                collEl.addContent(new Element("mods", MODS_V3_NS)
                        .addContent(new Element("identifier", MODS_V3_NS)
                                .setAttribute("displayLabel", CDM_NUMBER_LABEL)
                                .setAttribute("type", LOCAL_TYPE_VALUE)
                                .setText(cdmId)));
                cnt++;
            }

            xmlOutputter.output(doc, Files.newOutputStream(path));
        } catch (SQLException | IOException e) {
            throw new MigrationException("Failed to generate SIP", e);
        } finally {
            CdmIndexService.closeDbConnection(conn);
        }
        return cnt;
    }

    /**
     * @return Get the path to the file where generated MODS records are written
     */
    public Path getGeneratedModsPath() {
        return project.getDescriptionsPath().resolve(GENERATED_MODS_FILENAME);
    }

    /**
     * @param collId user supplied id for the new collection
     * @return Path for individual MODS file for a new destination collection
     */
    public Path getNewCollectionDescriptionPath(String collId) {
        return project.getNewCollectionDescriptionsPath().resolve(collId + ".xml");
    }

    public void setProject(MigrationProject project) {
        this.project = project;
    }
}
