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

import edu.unc.lib.boxc.deposit.impl.model.DepositDirectoryManager;
import edu.unc.lib.boxc.migration.cdm.model.MigrationSip;
import edu.unc.lib.boxc.model.api.rdf.Cdr;
import edu.unc.lib.boxc.model.api.rdf.CdrDeposit;
import org.apache.jena.rdf.model.Bag;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.RDF;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author bbpennel
 */
public class SipsCommandIT extends AbstractCommandIT {
    private final static String DEST_UUID = "3f3c5bcf-d5d6-46ad-87ec-bcdf1f06b19e";

    @Before
    public void setup() throws Exception {
        initProjectAndHelper();
    }

    @Test
    public void generateSourceFileNotMappedTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, null);
        testHelper.populateDescriptions("gilmer_mods1.xml");
        List<Path> stagingLocs = testHelper.populateSourceFiles("276_182_E.tif", "276_203_E.tif");
        Files.delete(stagingLocs.get(1));

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "sips", "generate" };
        executeExpectFailure(args);

        assertOutputContains("no source file has been mapped");

        // Re-run with force, which should produce SIP minus one missing file
        String[] argsF = new String[] {
                "-w", project.getProjectPath().toString(),
                "sips", "generate",
                "--force" };
        executeExpectSuccess(argsF);

        assertOutputContains("Cannot transform object 26, no source file has been mapped");

        MigrationSip sipF = extractSipFromOutput();

        DepositDirectoryManager dirManagerF = testHelper.createDepositDirectoryManager(sipF);
        Model modelF = testHelper.getSipModel(sipF);

        Bag depBagF = modelF.getBag(sipF.getDepositPid().getRepositoryPath());
        List<RDFNode> depBagChildrenF = depBagF.iterator().toList();
        assertEquals(2, depBagChildrenF.size());

        Resource workResc1F = testHelper.getResourceByCreateTime(depBagChildrenF, "2005-11-23");
        testHelper.assertObjectPopulatedInSip(workResc1F, dirManagerF, modelF, stagingLocs.get(0), null, "25");
        Resource workResc3F = testHelper.getResourceByCreateTime(depBagChildrenF, "2005-12-08");
        testHelper.assertObjectPopulatedInSip(workResc3F, dirManagerF, modelF, stagingLocs.get(1), null, "27");

        // Add missing file and rerun without force
        List<Path> stagingLocs2 = testHelper.populateSourceFiles("276_182_E.tif", "276_183_E.tif", "276_203_E.tif");

        executeExpectSuccess(args);

        MigrationSip sipR = extractSipFromOutput();

        DepositDirectoryManager dirManagerR = testHelper.createDepositDirectoryManager(sipR);
        Model modelR = testHelper.getSipModel(sipR);

        Bag depBagR = modelR.getBag(sipR.getDepositPid().getRepositoryPath());
        List<RDFNode> depBagChildrenR = depBagR.iterator().toList();
        assertEquals(3, depBagChildrenR.size());

        Resource workResc1R = testHelper.getResourceByCreateTime(depBagChildrenR, "2005-11-23");
        testHelper.assertObjectPopulatedInSip(workResc1R, dirManagerR, modelR, stagingLocs2.get(0), null, "25");
        Resource workResc2R = testHelper.getResourceByCreateTime(depBagChildrenR, "2005-11-24");
        testHelper.assertObjectPopulatedInSip(workResc2R, dirManagerR, modelR, stagingLocs2.get(1), null, "26");
        Resource workResc3R = testHelper.getResourceByCreateTime(depBagChildrenR, "2005-12-08");
        testHelper.assertObjectPopulatedInSip(workResc3R, dirManagerR, modelR, stagingLocs2.get(2), null, "27");
    }

    @Test
    public void generateValidTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, null);
        testHelper.populateDescriptions("gilmer_mods1.xml");
        List<Path> stagingLocs = testHelper.populateSourceFiles("276_182_E.tif", "276_183_E.tif", "276_203_E.tif");

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "sips", "generate" };
        executeExpectSuccess(args);

        MigrationSip sip = extractSipFromOutput();

        DepositDirectoryManager dirManager = testHelper.createDepositDirectoryManager(sip);
        Model model = testHelper.getSipModel(sip);

        Bag depBag = model.getBag(sip.getDepositPid().getRepositoryPath());
        List<RDFNode> depBagChildren = depBag.iterator().toList();
        assertEquals(3, depBagChildren.size());

        Resource workResc1 = testHelper.getResourceByCreateTime(depBagChildren, "2005-11-23");
        testHelper.assertObjectPopulatedInSip(workResc1, dirManager, model, stagingLocs.get(0), null, "25");
        Resource workResc2 = testHelper.getResourceByCreateTime(depBagChildren, "2005-11-24");
        testHelper.assertObjectPopulatedInSip(workResc2, dirManager, model, stagingLocs.get(1), null, "26");
        Resource workResc3 = testHelper.getResourceByCreateTime(depBagChildren, "2005-12-08");
        testHelper.assertObjectPopulatedInSip(workResc3, dirManager, model, stagingLocs.get(2), null, "27");

        String[] argsList = new String[] {
                "-w", project.getProjectPath().toString(),
                "sips", "list" };
        executeExpectSuccess(argsList);

        assertOutputContains("SIP/Deposit ID: " + sip.getDepositId());
        assertOutputContains("    Path: " + sip.getSipPath());
    }

    @Test
    public void generateWithNewCollectionTest() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        String newCollId = "00123test";
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, newCollId);
        testHelper.populateDescriptions("gilmer_mods1.xml");
        List<Path> stagingLocs = testHelper.populateSourceFiles("276_182_E.tif", "276_183_E.tif", "276_203_E.tif");

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "sips", "generate" };
        executeExpectSuccess(args);

        MigrationSip sip = extractSipFromOutput();
        assertEquals(newCollId, sip.getNewCollectionLabel());
        assertNotNull(sip.getNewCollectionPid());

        DepositDirectoryManager dirManager = testHelper.createDepositDirectoryManager(sip);
        Model model = testHelper.getSipModel(sip);

        Bag depBag = model.getBag(sip.getDepositPid().getRepositoryPath());
        List<RDFNode> depBagChildren = depBag.iterator().toList();
        assertEquals(1, depBagChildren.size());

        Resource collResc = depBagChildren.iterator().next().asResource();
        assertTrue(collResc.hasProperty(RDF.type, Cdr.Collection));
        assertTrue(collResc.hasProperty(CdrDeposit.label, newCollId));
        Bag collBag = model.getBag(collResc);
        List<RDFNode> collChildren = collBag.iterator().toList();

        Resource workResc1 = testHelper.getResourceByCreateTime(collChildren, "2005-11-23");
        testHelper.assertObjectPopulatedInSip(workResc1, dirManager, model, stagingLocs.get(0), null, "25");
        Resource workResc2 = testHelper.getResourceByCreateTime(collChildren, "2005-11-24");
        testHelper.assertObjectPopulatedInSip(workResc2, dirManager, model, stagingLocs.get(1), null, "26");
        Resource workResc3 = testHelper.getResourceByCreateTime(collChildren, "2005-12-08");
        testHelper.assertObjectPopulatedInSip(workResc3, dirManager, model, stagingLocs.get(2), null, "27");

        String[] argsList = new String[] {
                "-w", project.getProjectPath().toString(),
                "sips", "list" };
        executeExpectSuccess(argsList);

        assertOutputContains("SIP/Deposit ID: " + sip.getDepositId());
        assertOutputContains("    Path: " + sip.getSipPath());
        assertOutputContains("    New collection: " + sip.getNewCollectionLabel()
                + " (" + sip.getNewCollectionId() + ")");
    }

    private MigrationSip extractSipFromOutput() {
        return testHelper.extractSipFromOutput(output);
    }
}
