package edu.unc.lib.boxc.migration.cdm.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Destination mapping information for a project
 * @author bbpennel
 */
public class DestinationsInfo {
    public static final String DEFAULT_ID = "default";
    public static final String DESTINATION_FIELD = "boxc_dest";
    public static final String COLLECTION_FIELD = "new_collection";
    public static final String ID_FIELD = "id";
    public static final String[] CSV_HEADERS = new String[] {
            ID_FIELD, DESTINATION_FIELD, COLLECTION_FIELD };

    private List<DestinationMapping> mappings;

    public DestinationsInfo() {
        mappings = new ArrayList<>();
    }

    public List<DestinationMapping> getMappings() {
        return mappings;
    }

    public void setMappings(List<DestinationMapping> mappings) {
        this.mappings = mappings;
    }

    /**
     * An individual destination mapping
     * @author bbpennel
     */
    public static class DestinationMapping {
        private String id;
        private String destination;
        private String collectionId;

        public DestinationMapping() {
        }

        public DestinationMapping(String id, String destination, String collectionId) {
            this.id = id;
            this.destination = destination;
            this.collectionId = collectionId;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getDestination() {
            return destination;
        }

        public void setDestination(String destination) {
            this.destination = destination;
        }

        public String getCollectionId() {
            return collectionId;
        }

        public void setCollectionId(String collectionId) {
            this.collectionId = collectionId;
        }
    }
}
