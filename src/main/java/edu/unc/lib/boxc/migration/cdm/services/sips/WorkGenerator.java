package edu.unc.lib.boxc.migration.cdm.services.sips;

import edu.unc.lib.boxc.auth.api.UserRole;
import edu.unc.lib.boxc.deposit.impl.model.DepositModelHelpers;
import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.model.DestinationSipEntry;
import edu.unc.lib.boxc.migration.cdm.model.PermissionsInfo;
import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo;
import edu.unc.lib.boxc.migration.cdm.options.SipGenerationOptions;
import edu.unc.lib.boxc.migration.cdm.services.AccessFileService;
import edu.unc.lib.boxc.migration.cdm.services.DescriptionsService;
import edu.unc.lib.boxc.migration.cdm.services.PostMigrationReportService;
import edu.unc.lib.boxc.migration.cdm.services.RedirectMappingService;
import edu.unc.lib.boxc.migration.cdm.services.SipService;
import edu.unc.lib.boxc.model.api.DatastreamType;
import edu.unc.lib.boxc.model.api.ids.PID;
import edu.unc.lib.boxc.model.api.ids.PIDMinter;
import edu.unc.lib.boxc.model.api.rdf.Cdr;
import edu.unc.lib.boxc.model.api.rdf.CdrDeposit;
import org.apache.jena.rdf.model.Bag;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.RDF;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import static edu.unc.lib.boxc.auth.api.AccessPrincipalConstants.AUTHENTICATED_PRINC;
import static edu.unc.lib.boxc.auth.api.AccessPrincipalConstants.PUBLIC_PRINC;
import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;
import static edu.unc.lib.boxc.model.api.DatastreamType.ORIGINAL_FILE;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Base generator for works which are constructed for standalone CDM objects
 *
 * @author bbpennel
 */
public class WorkGenerator {
    private static final Logger log = getLogger(WorkGenerator.class);
    protected PIDMinter pidMinter;
    protected RedirectMappingService redirectMappingService;
    protected SourceFilesInfo sourceFilesInfo;
    protected SourceFilesInfo accessFilesInfo;
    protected Connection conn;
    protected SipGenerationOptions options;
    protected Model model;
    protected DestinationSipEntry destEntry;
    protected SipPremisLogger sipPremisLogger;
    protected DescriptionsService descriptionsService;
    protected AccessFileService accessFileService;
    protected PostMigrationReportService postMigrationReportService;
    protected PermissionsInfo permissionsInfo;

    protected String cdmId;
    protected String cdmCreated;

    protected PID workPid;
    protected Bag workBag;
    protected List<PID> fileObjPids;

    public void generate() throws IOException, SQLException {
        workPid = pidMinter.mintContentPid();
        workBag = null;

        Bag destBag = destEntry.getDestinationBag();
        model = destBag.getModel();

        generateWork();

        destBag.add(workBag);

        // Generate migration PREMIS event
        sipPremisLogger.addPremisEvent(destEntry, workPid, options);
        for (PID fileObjPid : fileObjPids) {
            sipPremisLogger.addPremisEvent(destEntry, fileObjPid, options);
        }
    }

    protected void generateWork() throws IOException {
        Path expDescPath = getDescriptionPath(cdmId, false);

        log.info("Transforming CDM object {} to box-c work {}", cdmId, workPid.getId());
        workBag = model.createBag(workPid.getRepositoryPath());
        workBag.addProperty(RDF.type, Cdr.Work);
        workBag.addLiteral(CdrDeposit.createTime, cdmCreated);

        // add permission to work
        addPermission(cdmId, workBag);

        // Copy description to SIP
        copyDescriptionToSip(workPid, expDescPath);

        fileObjPids = addChildObjects();
        postMigrationReportService.addWorkRow(cdmId, workPid.getId(), fileObjPids.size(), isSingleItem());
    }

    protected List<PID> addChildObjects() throws IOException {
        SourceFilesInfo.SourceFileMapping sourceMapping = getSourceFileMapping(cdmId);
        var fileObjectPid = addFileObject(cdmId, cdmCreated, sourceMapping);
        // in a single file object, the cdm id refers to the new work, so add suffix to reference the child file
        addChildDescription(cdmId + "/original_file", fileObjectPid);
        postMigrationReportService.addFileRow(cdmId + "/original_file", cdmId, workPid.getId(),
                fileObjectPid.getId(), isSingleItem());
        return Collections.singletonList(fileObjectPid);
    }

    protected void copyDescriptionToSip(PID pid, Path descPath) throws IOException {
        if (Files.notExists(descPath)) {
            return;
        }
        // Copy description to SIP
        Path sipDescPath = destEntry.getDepositDirManager().getModsPath(pid);
        Files.copy(descPath, sipDescPath);
    }

    protected Path getDescriptionPath(String cdmId, boolean allowMissing) {
        Path expDescPath = descriptionsService.getExpandedDescriptionFilePath(cdmId);
        if (Files.notExists(expDescPath)) {
            if (allowMissing) {
                return null;
            }
            String message = "Cannot transform object " + cdmId + ", it does not have a MODS description";
            if (options.isForce()) {
                outputLogger.info(message);
                throw new SipService.SkipObjectException();
            } else {
                throw new InvalidProjectStateException(message);
            }
        }
        return expDescPath;
    }

    protected SourceFilesInfo.SourceFileMapping getSourceFileMapping(String cdmId) {
        SourceFilesInfo.SourceFileMapping sourceMapping = sourceFilesInfo.getMappingByCdmId(cdmId);
        if (sourceMapping == null || sourceMapping.getSourcePaths() == null) {
            String message = "Cannot transform object " + cdmId + ", no source file has been mapped";
            if (options.isForce()) {
                outputLogger.info(message);
                throw new SipService.SkipObjectException();
            } else {
                throw new InvalidProjectStateException(message);
            }
        }
        return sourceMapping;
    }

    protected Resource makeFileResource(PID fileObjPid, Path sourcePath) {
        Resource fileObjResc = model.getResource(fileObjPid.getRepositoryPath());
        fileObjResc.addProperty(RDF.type, Cdr.FileObject);

        workBag.add(fileObjResc);

        // Link source file
        Resource origResc = DepositModelHelpers.addDatastream(fileObjResc, ORIGINAL_FILE);
        origResc.addLiteral(CdrDeposit.stagingLocation, sourcePath.toUri().toString());
        origResc.addLiteral(CdrDeposit.label, sourcePath.getFileName().toString());
        return fileObjResc;
    }

    protected PID addFileObject(String cdmId, String cdmFileCreated, SourceFilesInfo.SourceFileMapping sourceMapping)
            throws IOException {
        // Create FileObject with source file
        PID fileObjPid = pidMinter.mintContentPid();
        Resource fileObjResc = makeFileResource(fileObjPid, sourceMapping.getFirstSourcePath());
        fileObjResc.addLiteral(CdrDeposit.createTime, cdmFileCreated);

        // Add permission to source file
        addPermission(cdmId, fileObjResc);

        // Link access file
        if (accessFilesInfo != null) {
            SourceFilesInfo.SourceFileMapping accessMapping = accessFilesInfo.getMappingByCdmId(cdmId);
            if (accessMapping != null && accessMapping.getSourcePaths() != null) {
                Resource accessResc = DepositModelHelpers.addDatastream(
                        fileObjResc, DatastreamType.ACCESS_SURROGATE);
                accessResc.addLiteral(CdrDeposit.stagingLocation,
                        accessMapping.getFirstSourcePath().toUri().toString());
                String mimetype = accessFileService.getMimetype(accessMapping.getFirstSourcePath());
                accessResc.addLiteral(CdrDeposit.mimetype, mimetype);
            }
        }

        // add redirect mapping for this file
        redirectMappingService.addRow(cdmId, workPid.getId(), fileObjPid.getId());

        return fileObjPid;
    }

    /**
     * Copy description file into place for child object, if it exists
     * @param descCdmId CDM id of the child object used for associating the description
     * @param fileObjPid pid of the file object
     * @throws IOException
     */
    protected void addChildDescription(String descCdmId, PID fileObjPid) throws IOException {
        var childDescPath = getDescriptionPath(descCdmId, true);
        if (childDescPath != null) {
            copyDescriptionToSip(fileObjPid, childDescPath);
        }
    }

    /**
     * @return true if the object being transformed is a single item CDM object
     */
    protected boolean isSingleItem() {
        return true;
    }

    protected void addPermission(String cdmId, Resource resource) {
        if (permissionsInfo != null) {
            PermissionsInfo.PermissionMapping permissionMapping = permissionsInfo.getMappingByCdmId(cdmId);
            if (permissionMapping != null) {
                Property everyoneValue = UserRole.valueOf(permissionMapping.getEveryone()).getProperty();
                Property authenticatedValue = UserRole.valueOf(permissionMapping.getAuthenticated()).getProperty();
                resource.addLiteral(everyoneValue, PUBLIC_PRINC);
                resource.addLiteral(authenticatedValue, AUTHENTICATED_PRINC);
            }
        }
    }
}
