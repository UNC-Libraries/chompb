package edu.unc.lib.boxc.migration.cdm.services.sips;

import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo;
import edu.unc.lib.boxc.model.api.ids.PID;
import edu.unc.lib.boxc.model.api.rdf.Cdr;
import edu.unc.lib.boxc.operations.api.order.MemberOrderHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Generator for works which capture the order of their children
 *
 * @author bbpennel
 */
public class OrderedWorkGenerator extends MultiFileWorkGenerator {
    private SourceFilesInfo aggregateTopMappings;
    private SourceFilesInfo aggregateBottomMappings;

    @Override
    protected List<PID> addChildObjects() throws IOException {
        var childrenPids = super.addChildObjects();
        var topAggregates = makeAggregateFileObjects(aggregateTopMappings);
        var bottomAggregates = makeAggregateFileObjects(aggregateBottomMappings);
        var mergedPids = new ArrayList<PID>(topAggregates.size() + childrenPids.size() + bottomAggregates.size());
        mergedPids.addAll(topAggregates);
        mergedPids.addAll(childrenPids);
        mergedPids.addAll(bottomAggregates);
        storeChildrenOrder(childrenPids);
        return childrenPids;
    }

    private List<PID> makeAggregateFileObjects(SourceFilesInfo aggregateMappings) {
        var mapping = aggregateTopMappings.getMappingByCdmId(cdmId);
        if (mapping.getSourcePaths() == null) {
            return Collections.emptyList();
        }
        // Make a file resource for each sourcePath and compile a list of the new PIDs
        return mapping.getSourcePaths().stream().map(sourcePath -> {
                PID aggrPid = pidMinter.mintContentPid();
                makeFileResource(aggrPid, sourcePath);
                return aggrPid;
        }).collect(Collectors.toList());
    }

    protected void storeChildrenOrder(List<PID> fileObjPids) {
        // skip single file works to avoid creating unnecessary order properties
        if (fileObjPids.size() <= 1) {
            return;
        }
        workBag.addProperty(Cdr.memberOrder, MemberOrderHelper.serializeOrder(fileObjPids));
    }

    public void setAggregateTopMappings(SourceFilesInfo aggregateTopMappings) {
        this.aggregateTopMappings = aggregateTopMappings;
    }

    public void setAggregateBottomMappings(SourceFilesInfo aggregateBottomMappings) {
        this.aggregateBottomMappings = aggregateBottomMappings;
    }
}
