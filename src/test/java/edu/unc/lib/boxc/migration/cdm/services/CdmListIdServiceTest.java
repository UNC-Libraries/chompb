package edu.unc.lib.boxc.migration.cdm.services;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.List;

public class CdmListIdServiceTest {
    private static final String CDM_BASE_URL = "http://example.com:88/";
    private static final String PROJECT_NAME = "proj";
    @Rule
    public final TemporaryFolder tmpFolder = new TemporaryFolder();

    private MigrationProject project;
    private CdmListIdService service;
    @Mock
    private CloseableHttpClient httpClient;
    @Mock
    private CloseableHttpResponse httpResp;
    @Mock
    private HttpEntity respEntity;
    @Captor
    private ArgumentCaptor<HttpGet> httpGetArgumentCaptor;

    @Before
    public void setup() throws Exception {
        initMocks(this);
        tmpFolder.create();
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder.getRoot().toPath(), PROJECT_NAME, null, "user");
        service = new CdmListIdService();
        service.setHttpClient(httpClient);
        service.setCdmBaseUri(CDM_BASE_URL);

        when(httpClient.execute(any(HttpGet.class))).thenReturn(httpResp);
        when(httpResp.getEntity()).thenReturn(respEntity);
    }

    @Test
    public void test() throws Exception {
        CdmListIdService listId = new CdmListIdService();
        listId.setCdmBaseUri("https://dc.lib.unc.edu:82/");
        CloseableHttpClient httpClient = HttpClients.createMinimal();
        listId.setHttpClient(httpClient);
        listId.listAllCdmId(project);
    }

    @Test
    public void retrieveCdmListIdTest() throws Exception {
        when(respEntity.getContent()).thenReturn(this.getClass().getResourceAsStream("/sample_pages/cdm_listid_resp.json"))
                .thenReturn(this.getClass().getResourceAsStream("/sample_pages/page_1.json"))
                .thenReturn(this.getClass().getResourceAsStream("/sample_pages/page_2.json"))
                .thenReturn(this.getClass().getResourceAsStream("/sample_pages/page_3.json"))
                .thenReturn(this.getClass().getResourceAsStream("/sample_pages/page_4.json"));
        List<String> allListId = service.listAllCdmId(project);

        assertEquals(161, allListId.size());
        assertEquals("25", allListId.get(0));
        assertEquals("130", allListId.get(99));
        assertEquals("131", allListId.get(100));
        assertEquals("193", allListId.get(160));

        assertUrlsCalled("https://dc.lib.unc.edu:82/dmwebservices/index.php?q=dmQuery/gilmer/0/dmrecord/dmrecord/1/0/1/0/0/0/0/json");
    }

    @Test
    public void retrieveCdmListIdNoResults() throws Exception {
        CdmListIdService listId = new CdmListIdService();
        List<String> allListId = listId.listAllCdmId(project);

        assertEquals(0, allListId.size());
        assertTrue(allListId.isEmpty());
    }

    @Test
    public void retrieveCdmListIdLessThanPageSize() throws Exception {
        when(respEntity.getContent()).thenReturn(this.getClass().getResourceAsStream("/sample_pages/cdm_listid_resp.json"))
                .thenReturn(this.getClass().getResourceAsStream("/sample_pages/page_all.json"));
        List<String> allListId = service.listAllCdmId(project);

        assertEquals(161, allListId.size());
        assertEquals("25", allListId.get(0));
        assertEquals("130", allListId.get(99));
        assertEquals("131", allListId.get(100));
        assertEquals("193", allListId.get(160));
    }

    @Test
    public void  retrieveCdmListIdMoreThanPageSize() throws Exception {

    }

    private void assertUrlsCalled(String... expectedUrls) throws Exception {
        verify(httpClient, Mockito.times(expectedUrls.length)).execute(httpGetArgumentCaptor.capture());
        List<HttpGet> getObjects = httpGetArgumentCaptor.getAllValues();
        for (int i = 0; i < expectedUrls.length; i++) {
            HttpGet getMethod = getObjects.get(i);
            String expectedUrl = expectedUrls[i];
            assertEquals(expectedUrl, getMethod.getURI().toString());
        }
    }
}
