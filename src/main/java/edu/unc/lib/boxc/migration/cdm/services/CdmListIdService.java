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

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;

import edu.unc.lib.boxc.common.util.URIUtil;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;

/**
 * Service for listing CDM object IDs
 * @author krwong
 */

public class CdmListIdService {
    private static final Logger log = getLogger(CdmListIdService.class);

    private CloseableHttpClient httpClient;
    private String cdmBaseUri;

    private int pageSize = 1000; //set default page size

    /**
     * Get the total number of object IDs for the given collection
     * @param project
     * @throw IOException
     * @return
     */
    private int getTotalObjects(MigrationProject project) throws IOException {
        String collectionId = project.getProjectProperties().getCdmCollectionId();
        String totalObjectsUrl = "dmwebservices/index.php?q=dmQuery/" + collectionId + "/0/dmrecord/dmrecord/1/0/1/0/0/0/0/json";
        String totalUri = URIUtil.join(cdmBaseUri, totalObjectsUrl);

        ObjectMapper mapper = new ObjectMapper();
        HttpGet getMethod = new HttpGet(totalUri);
        try (CloseableHttpResponse resp = httpClient.execute(getMethod)) {
            //Error looking up collection
            String body = IOUtils.toString(resp.getEntity().getContent(), ISO_8859_1);
            if (body.contains("Error looking up collection")) {
                throw new MigrationException("No collection with ID '" + collectionId
                        + "' found on server at " + totalUri);
            }
            JsonParser parser = mapper.getFactory().createParser(body);
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                throw new MigrationException("Unexpected response from URL " + totalUri
                        + "\nIt must be a JSON object, please check the response.");
            }
                ObjectNode entryNode = mapper.readTree(parser);
                JsonNode pager = entryNode.get("pager");
                int total = pager.get("total").asInt();
                return total;
        } catch (JsonParseException e) {
            throw new MigrationException("Failed to parse response from URL " + totalUri
                    + ": " + e.getMessage());
        }
    }

    /**
     * Page through the results
     * @param project
     * @throw IOException
     * @return
     */
    private List<String> pagingResults (MigrationProject project, int total) {
        int totalRecords = total;
        int pageSize = this.pageSize;
        int maxPages = totalRecords / pageSize;

        List<String> urls = new ArrayList<String>();

        for (int page = 0; page <= maxPages; page++) {
            int recordNum = (page * pageSize) + 1;
            String cdmPageUrl = "dmwebservices/index.php?q=dmQuery/" + project.getProjectProperties().getCdmCollectionId() + "/0/dmrecord/dmrecord/" + pageSize + "/" + recordNum + "/1/0/0/0/0/json";
            String pageUrl = URIUtil.join(cdmBaseUri, cdmPageUrl);
            urls.add(pageUrl);
        }
        return urls;
    }

    /**
     * Parse json for object IDs
     * @param project
     * @throw IOException
     * @return
     */
    private List<String> parseJson (MigrationProject project, String url) throws IOException {
        String collectionId = project.getProjectProperties().getCdmCollectionId();
        String objectUri = url;

        ArrayList<String> objectIds = new ArrayList<String>();

        ObjectMapper mapper = new ObjectMapper();
        HttpGet getMethod = new HttpGet(objectUri);
        try (CloseableHttpResponse resp = httpClient.execute(getMethod)) {
            //Error looking up collection
            String body = IOUtils.toString(resp.getEntity().getContent(), ISO_8859_1);
            if (body.contains("Error looking up collection")) {
                throw new MigrationException("No collection with ID '" + collectionId
                        + "' found on server at " + objectUri);
            }
            JsonParser parser = mapper.getFactory().createParser(body);
            while (parser.nextToken() == JsonToken.START_OBJECT) {
                ObjectNode entryNode = mapper.readTree(parser);
                ArrayNode records = (ArrayNode)entryNode.get("records");
                for (JsonNode record : records) {
                    String objectId = record.get("dmrecord").asText();
                    objectIds.add(objectId);
                }
            }
        } catch (JsonParseException e) {
            throw new MigrationException("Failed to parse response from URL " + objectUri
                    + ": " + e.getMessage());
        }
        return objectIds;
    }

    /**
     * List all exported CDM records for this project
     * @param project
     * "dmwebservices/index.php?q=dmQuery/" + collectionId + "/0/dmrecord/dmrecord/1024/" + start + "/1/0/0/0/0" + "/json"
     * @return
     */
    public List<String> listAllCdmId(MigrationProject project) throws IOException {
        int totalObjects = getTotalObjects(project);
        List<String> pageUrls = pagingResults(project, totalObjects);

        List<String> allObjectIds = new ArrayList<String>();

        for(String url : pageUrls) {
            List<String> objectIds = parseJson(project, url);
            allObjectIds.addAll(objectIds);
        }
        return allObjectIds;

//        System.out.println(totalObjects);
//        System.out.println(pageUrls);
//        System.out.println(allObjectIds);
    }

    public void setHttpClient(CloseableHttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public void setCdmBaseUri(String cdmBaseUri) {
        this.cdmBaseUri = cdmBaseUri;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

}

