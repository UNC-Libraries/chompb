package edu.unc.lib.boxc.migration.cdm.model;

import static edu.unc.lib.boxc.auth.api.AccessPrincipalConstants.AUTHENTICATED_PRINC;
import static edu.unc.lib.boxc.auth.api.AccessPrincipalConstants.PUBLIC_PRINC;
import static org.slf4j.LoggerFactory.getLogger;

import java.nio.file.Path;

import org.apache.commons.lang3.StringUtils;
import org.apache.jena.rdf.model.Bag;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.vocabulary.RDF;
import org.slf4j.Logger;

import edu.unc.lib.boxc.deposit.impl.model.DepositDirectoryManager;
import edu.unc.lib.boxc.deposit.impl.model.DepositModelManager;
import edu.unc.lib.boxc.migration.cdm.model.DestinationsInfo.DestinationMapping;
import edu.unc.lib.boxc.migration.cdm.services.SipService;
import edu.unc.lib.boxc.model.api.ids.PID;
import edu.unc.lib.boxc.model.api.ids.PIDMinter;
import edu.unc.lib.boxc.model.api.rdf.Cdr;
import edu.unc.lib.boxc.model.api.rdf.CdrAcl;
import edu.unc.lib.boxc.model.api.rdf.CdrDeposit;
import edu.unc.lib.boxc.model.fcrepo.ids.PIDs;

/**
 * Object containing state information related to the destination of a SIP
 * @author bbpennel
 */
public class DestinationSipEntry {
    private static final Logger log = getLogger(DestinationSipEntry.class);
    private PID depositPid;
    private PID newCollectionPid;
    private String newCollectionId;
    private PID destinationPid;
    DepositModelManager depositModelManager;
    DepositDirectoryManager depositDirManager;
    private Model writeModel;

    public DestinationSipEntry(PID depositPid, DestinationMapping mapping, Path sipPath, PIDMinter pidMinter) {
        this.depositPid = depositPid;
        this.depositDirManager = new DepositDirectoryManager(depositPid, sipPath, true);
        if (!StringUtils.isBlank(mapping.getCollectionId())) {
            this.newCollectionPid = pidMinter.mintContentPid();
            this.newCollectionId = mapping.getCollectionId();
            log.info("Generated new collection {} from id {}", newCollectionPid.getId(), mapping.getCollectionId());
        }
        this.depositModelManager = new DepositModelManager(getTdbPath());
        this.destinationPid = PIDs.get(mapping.getDestination());
    }

    public void initializeDepositModel() {
        Model model = getWriteModel();
        Bag depRootBag = model.createBag(depositPid.getRepositoryPath());
        // Populate the new collection object
        if (newCollectionPid != null) {
            Bag newCollBag = model.createBag(newCollectionPid.getRepositoryPath());
            depRootBag.add(newCollBag);
            newCollBag.addProperty(RDF.type, Cdr.Collection);
            newCollBag.addLiteral(CdrDeposit.label, newCollectionId);
            // New collections should be created as publicly accessible
            newCollBag.addProperty(CdrAcl.none, PUBLIC_PRINC);
            newCollBag.addProperty(CdrAcl.none, AUTHENTICATED_PRINC);
        }
        commitModel();
    }

    public Path getTdbPath() {
        return depositDirManager.getDepositDir().resolve(SipService.SIP_TDB_PATH);
    }

    public Model getWriteModel() {
        if (writeModel == null) {
            writeModel = depositModelManager.getWriteModel(depositPid);
        }
        return writeModel;
    }

    public void commitModel() {
        depositModelManager.commit();
        writeModel = null;
    }

    public void close() {
        depositModelManager.close();
    }

    public Bag getDestinationBag() {
        if (newCollectionPid != null) {
            return getWriteModel().getBag(newCollectionPid.getRepositoryPath());
        } else {
            return getWriteModel().getBag(depositPid.getRepositoryPath());
        }
    }

    public PID getDepositPid() {
        return depositPid;
    }

    public PID getNewCollectionPid() {
        return newCollectionPid;
    }

    public String getNewCollectionId() {
        return newCollectionId;
    }

    public PID getDestinationPid() {
        return destinationPid;
    }

    public DepositModelManager getDepositModelManager() {
        return depositModelManager;
    }

    public DepositDirectoryManager getDepositDirManager() {
        return depositDirManager;
    }
}