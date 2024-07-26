package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo;
import edu.unc.lib.boxc.migration.cdm.options.GenerateSourceFileMappingOptions;
import edu.unc.lib.boxc.migration.cdm.options.SourceFileMappingOptions;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static edu.unc.lib.boxc.migration.cdm.services.CdmIndexService.ENTRY_TYPE_FIELD;

/**
 * Service which allows mapping of aggregate files like PDFs or TXTs to multi-file works
 * @author bbpennel
 */
public class AggregateFileMappingService extends SourceFileService {
    private boolean sortBottom;

    public AggregateFileMappingService(boolean sortBottom) {
        this.sortBottom = sortBottom;
    }

    @Override
    public Path getMappingPath() {
        if (sortBottom) {
            return project.getAggregateBottomMappingPath();
        } else {
            return project.getAggregateTopMappingPath();
        }
    }

    // Query for grouped works or compound objects (no children or single file works)
    @Override
    protected String buildQuery(GenerateSourceFileMappingOptions options) {
        String selectStatement;
        if (options.isPopulateBlank()) {
            selectStatement = "select " + CdmFieldInfo.CDM_ID;
        } else {
            selectStatement = "select " + CdmFieldInfo.CDM_ID + ", " + options.getExportField();
        }
        return selectStatement
                + " from " + CdmIndexService.TB_NAME
                + " where " + ENTRY_TYPE_FIELD + " = '" + CdmIndexService.ENTRY_TYPE_COMPOUND_OBJECT + "'"
                + " or " + ENTRY_TYPE_FIELD + " = '" + CdmIndexService.ENTRY_TYPE_GROUPED_WORK + "'";
    }

    @Override
    protected SourceFilesInfo.SourceFileMapping resolveSourcePathConflict(GenerateSourceFileMappingOptions options,
                                                                          SourceFilesInfo.SourceFileMapping origMapping,
                                                                          SourceFilesInfo.SourceFileMapping updateMapping) {
        if (options.isForce() || origMapping.getSourcePaths() == null) {
            return updateMapping;
        }
        // Combine the old and new values, removing any duplicates
        List<Path> combined = Stream.concat(origMapping.getSourcePaths().stream(),
                        updateMapping.getSourcePaths().stream())
                .distinct()
                .collect(Collectors.toList());
        updateMapping.setSourcePaths(combined);
        return updateMapping;
    }

    public void setSortBottom(boolean sortBottom) {
        this.sortBottom = sortBottom;
    }
}
