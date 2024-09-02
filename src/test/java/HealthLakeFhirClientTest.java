
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import lib.hios.orch.HealthLakeFhirClient;

public class HealthLakeFhirClientTest {
    private HealthLakeFhirClient client;

    private static final String ENDPOINT_URL = System.getenv("ENDPOINT_URL");
    private static final String DATASTORE_ID = System.getenv("HEALTHLAKE_DATASTORE_ID");
    private static final String ACCESS_KEY_ID = System.getenv("ACCESS_KEY_ID");
    private static final String SECRET_ACCESS_KEY = System.getenv("SECRET_ACCESS_KEY");

    @BeforeEach
    public void setUp() {
        client = new HealthLakeFhirClient(ENDPOINT_URL, DATASTORE_ID, ACCESS_KEY_ID, SECRET_ACCESS_KEY);
    }

    @Test
    public void testSendRequest_GetRequest() throws Exception {
        String response = client.sendRequest("GET", "Patient", "2de04858-ba65-44c1-8af1-f2fe69a977d9", "");
        assertNotNull(response, "GET request failed; response is null.");
        System.out.println("GET Response: " + response);
    }

    @Test
    public void testSendRequest_PostRequest() throws Exception {
        String payload = "{\n\t\"id\": \"2de04858-ba65-44c1-8af1-f2fe69a977d9\",\n\t\"resourceType\": \"Patient\",\n\t\"active\": true,\n\t\"name\": [{\n\t\t\t\"use\": \"official\",\n\t\t\t\"family\": \"shaft\",\n\t\t\t\"given\": [\n\t\t\t\t\"mukky\"\n\t\t\t]\n\t\t},\n\t\t{\n\t\t\t\"use\": \"usual\",\n\t\t\t\"given\": [\n\t\t\t\t\"Jane\"\n\t\t\t]\n\t\t}\n\t],\n\t\"gender\": \"female\",\n\t\"birthDate\": \"1985-12-31\",\n\t\"meta\": {\n\t\t\"lastUpdated\": \"2020-11-23T06:43:45.133Z\"\n\t}\n}"; // Example
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   // payload
        String response = client.sendRequest("POST", "Patient", "", payload);
        assertNotNull(response, "POST request failed; response is null.");
        System.out.println("POST Response: " + response);
    }

    @Test
    public void testSendRequest_PutRequest() throws Exception {
        String payload = "{\n\t\"id\": \"2de04858-ba65-44c1-8af1-f2fe69a977d9\",\n\t\"resourceType\": \"Patient\",\n\t\"active\": true,\n\t\"name\": [{\n\t\t\t\"use\": \"official\",\n\t\t\t\"family\": \"shaft\",\n\t\t\t\"given\": [\n\t\t\t\t\"mukky\"\n\t\t\t]\n\t\t},\n\t\t{\n\t\t\t\"use\": \"usual\",\n\t\t\t\"given\": [\n\t\t\t\t\"Jane\"\n\t\t\t]\n\t\t}\n\t],\n\t\"gender\": \"female\",\n\t\"birthDate\": \"1985-12-31\",\n\t\"meta\": {\n\t\t\"lastUpdated\": \"2020-11-23T06:43:45.133Z\"\n\t}\n}"; // Example
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   // payload
        String resourceId = "2de04858-ba65-44c1-8af1-f2fe69a977d9";
        String response = client.sendRequest("PUT", "Patient", resourceId, payload);
        assertNotNull(response, "PUT request failed; response is null.");
        System.out.println("PUT Response: " + response);
    }

    @Test
    public void testSendRequest_DeleteRequest() throws Exception {
        String response = client.sendRequest("DELETE", "Patient", "2de04858-ba65-44c1-8af1-f2fe69a977d9", "");
        assertNotNull(response, "DELETE request failed; response is null.");
        System.out.println("DELETE Response: " + response);
    }
}
