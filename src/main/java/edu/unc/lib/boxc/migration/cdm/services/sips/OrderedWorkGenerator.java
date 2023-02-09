package edu.unc.lib.boxc.migration.cdm.services.sips;

import edu.unc.lib.boxc.model.api.ids.PID;
import edu.unc.lib.boxc.model.api.rdf.Cdr;
import edu.unc.lib.boxc.operations.api.order.MemberOrderHelper;

import java.io.IOException;
import java.util.List;

/**
 * Generator for works which capture the order of their children
 *
 * @author bbpennel
 */
public class OrderedWorkGenerator extends MultiFileWorkGenerator {
    @Override
    protected List<PID> addChildObjects() throws IOException {
        var childrenPids = super.addChildObjects();
        storeChildrenOrder(childrenPids);
        return childrenPids;
    }

    protected void storeChildrenOrder(List<PID> fileObjPids) {
        // skip single file works to avoid creating unnecessary order properties
        if (fileObjPids.size() <= 1) {
            return;
        }
        workBag.addProperty(Cdr.memberOrder, MemberOrderHelper.serializeOrder(fileObjPids));
    }
}
