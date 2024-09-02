package lib.hios.orch;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Base64;

/**
 * A client for interacting with AWS HealthLake FHIR Datastore.
 * 
 * <p>
 * This class provides functionality to make HTTP requests to the AWS HealthLake
 * FHIR Datastore API. Due to limitations or constraints that prevented the use
 * of
 * the AWS SDK for Java, the client manually signs the requests using AWS
 * Signature
 * Version 4 (SigV4) authentication. This approach involves constructing the
 * required
 * canonical request, generating a string to sign, and computing the signature
 * using
 * HMAC-SHA256.
 * </p>
 * 
 * <p>
 * Methods in this class support HTTP methods like GET, POST, PUT, and DELETE,
 * and
 * handle the construction of the necessary headers for authentication and
 * content
 * types. The client is designed to interact with AWS HealthLake using its REST
 * API
 * directly through HTTP, providing flexibility and control over the
 * request-building
 * and signing process.
 * </p>
 * 
 * <p>
 * Note: It was not possible to use the AWS SDK for Java due to specific
 * constraints,
 * so this manual approach was necessary to directly communicate with the AWS
 * HealthLake
 * API endpoint.
 * </p>
 */

public class HealthLakeFhirClient {
    private final String endpointUrl;
    private final String datastoreId;
    private final String accessKeyId;
    private final String secretAccessKey;

    public HealthLakeFhirClient(final String endpointUrl, final String datastoreId, final String accessKeyId,
            final String secretAccessKey) {
        this.endpointUrl = endpointUrl;
        this.datastoreId = datastoreId;
        this.accessKeyId = accessKeyId;
        this.secretAccessKey = secretAccessKey;
    }

    /**
     * Sends an HTTP request to the AWS HealthLake FHIR Datastore.
     *
     * @param httpMethod   The HTTP method (GET, POST, PUT, DELETE).
     * @param resourceType The type of the FHIR resource (e.g., Patient,
     *                     Observation).
     * @param resourceId   The ID of the FHIR resource (optional, can be empty).
     * @param payload      The request payload, typically a JSON representation of
     *                     the FHIR resource.
     * @return The response body as a string.
     * @throws Exception If an error occurs during request signing or sending.
     */

    public String sendRequest(final String httpMethod, final String resourceType, final String resourceId,
            final String payload)
            throws Exception {
        // Construct the URL
        final var url = endpointUrl + "/datastore/" + datastoreId + "/r4/" + resourceType;
        final var finalUrl = resourceId.isEmpty() ? url : url + "/" + resourceId;
        final var uri = new URI(finalUrl);

        final var amzDate = generateAmzDate();
        final var dateStamp = amzDate.substring(0, 8);

        final var canonicalUri = uri.getPath();
        final var canonicalQueryString = uri.getQuery() != null ? uri.getQuery() : "";
        final var canonicalHeaders = "host:" + uri.getHost() + "\n" + "x-amz-date:" + amzDate + "\n";
        final var signedHeaders = "host;x-amz-date";

        final var payloadHash = hash(payload);

        final var canonicalRequest = httpMethod + "\n" +
                canonicalUri + "\n" +
                canonicalQueryString + "\n" +
                canonicalHeaders + "\n" +
                signedHeaders + "\n" +
                payloadHash;

        System.out.println("Canonical Request:\n" + canonicalRequest);

        final var algorithm = "AWS4-HMAC-SHA256";
        final var credentialScope = dateStamp + "/us-east-2/healthlake/aws4_request";
        final var stringToSign = algorithm + "\n" +
                amzDate + "\n" +
                credentialScope + "\n" +
                hash(canonicalRequest);

        System.out.println("String to Sign:\n" + stringToSign);

        final var signingKey = getSignatureKey(secretAccessKey, dateStamp, "us-east-2", "healthlake");

        final var signature = toHex(hmacSHA256(stringToSign, signingKey));

        System.out.println("Signature: " + signature);

        final var authorizationHeader = algorithm + " " +
                "Credential=" + accessKeyId + "/" + credentialScope + ", " +
                "SignedHeaders=" + signedHeaders + ", " +
                "Signature=" + signature;

        final var requestBuilder = HttpRequest.newBuilder()
                .uri(uri)
                .header("Accept", "application/json")
                .header("Content-Type", "application/fhir+json")
                .header("X-Amz-Date", amzDate)
                .header("Authorization", authorizationHeader);

        switch (httpMethod.toUpperCase()) {
            case "POST":
                requestBuilder.POST(HttpRequest.BodyPublishers.ofString(payload));
                break;
            case "PUT":
                requestBuilder.PUT(HttpRequest.BodyPublishers.ofString(payload));
                break;
            case "DELETE":
                requestBuilder.DELETE();
                break;
            case "GET":
            default:
                requestBuilder.GET();
                break;
        }

        final var request = requestBuilder.build();
        System.out.println("Request: " + request);

        final var client = HttpClient.newHttpClient();
        final var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        System.out.println("Response Headers: " + response.headers());
        System.out.println("Response Body: " + response.body());

        if (response.statusCode() != 200 && response.statusCode() != 201 && response.statusCode() != 204) {
            throw new RuntimeException("HTTP request failed: " + response.statusCode() + " " + response.body());
        }

        return response.body();
    }

    private static String generateAmzDate() {
        final var amzDateFormatter = new DateTimeFormatterBuilder()
                .appendValue(ChronoField.YEAR, 4)
                .appendValue(ChronoField.MONTH_OF_YEAR, 2)
                .appendValue(ChronoField.DAY_OF_MONTH, 2)
                .appendLiteral('T')
                .appendValue(ChronoField.HOUR_OF_DAY, 2)
                .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
                .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
                .appendLiteral('Z')
                .toFormatter();
        final var nowUtc = ZonedDateTime.now().withZoneSameInstant(java.time.ZoneOffset.UTC);
        return nowUtc.format(amzDateFormatter);
    }

    private static byte[] hmacSHA256(final String data, final byte[] key) throws Exception {
        final var algorithm = "HmacSHA256";
        final var mac = Mac.getInstance(algorithm);
        mac.init(new SecretKeySpec(key, algorithm));
        return mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] getSignatureKey(final String key, final String dateStamp, final String regionName,
            final String serviceName)
            throws Exception {
        final var kDate = hmacSHA256(dateStamp, ("AWS4" + key).getBytes(StandardCharsets.UTF_8));
        final var kRegion = hmacSHA256(regionName, kDate);
        final var kService = hmacSHA256(serviceName, kRegion);
        return hmacSHA256("aws4_request", kService);
    }

    private static String hash(final String text) throws Exception {
        final var digest = MessageDigest.getInstance("SHA-256");
        final var hash = digest.digest(text.getBytes(StandardCharsets.UTF_8));
        return toHex(hash);
    }

    private static String toHex(final byte[] bytes) {
        final var sb = new StringBuilder();
        for (final var b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
