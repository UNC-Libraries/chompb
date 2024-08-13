package edu.unc.lib.boxc.migration.cdm.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Exported objects info for a project
 * @author krwong
 */
public class ExportObjectsInfo {
    public static final String RECORD_ID = CdmFieldInfo.CDM_ID;
    public static final String FILE_PATH = "file_path";
    public static final String FILENAME = "filename";
    public static final String[] CSV_HEADERS = new String[] {RECORD_ID, FILE_PATH, FILENAME};

    private List<ExportedObject> objects;

    public ExportObjectsInfo() {
        objects = new ArrayList<>();
    }

    public List<ExportedObject> getObjects() {
        return objects;
    }

    public void setObjects(List<ExportedObject> objects) {
        this.objects = objects;
    }

    public static class ExportedObject {
        private String recordId;
        private String filePath;
        private String filename;

        public String getRecordId() {
            return recordId;
        }

        public void setRecordId(String recordId) {
            this.recordId = recordId;
        }

        public String getFilePath() {
            return filePath;
        }

        public void setFilePath(String filePath) {
            this.filePath = filePath;
        }

        public String getFilename() {
            return filename;
        }

        public void setFilename(String filename) {
            this.filename = filename;
        }
    }
}
