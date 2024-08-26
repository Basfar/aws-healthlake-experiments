import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.UUID;
import java.util.function.Consumer;

import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.VFS;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CreateBucketRequest;

import lib.hios.orch.HealthInfoOrchestrator;
import lib.hios.orch.HealthInfoOrchestrator.AwsCliHealthLakeFhirStore;
import lib.hios.orch.HealthInfoOrchestrator.AwsS3ContentStore;

public class HealthInfoOrchestratorTest {
    private HealthInfoOrchestrator orchestrator;
    private AwsS3ContentStore contentStore;
    private AwsCliHealthLakeFhirStore fhirStore;

    @TempDir
    File tempDir;

    // private static final String bucketName = System.getenv("AWS_S3_BUCKET_NAME");
    // //if bucket exists
    private String bucketName; // if bucket don't exists
    private static final String region = System.getenv("AWS_REGION");
    private static final String datastoreId = System.getenv("HEALTHLAKE_DATASTORE_ID");
    private static final String roleArn = System.getenv("HEALTHLAKE_ROLE_ARN");
    private static final String kmsKeyId = System.getenv("KMS_KEY_ID");

    @BeforeEach
    void setup() throws FileSystemException {
        // assertThat(bucketName).as("AWS_S3_BUCKET_NAME").isNotEmpty(); //if bucket
        // exists
        assertThat(region).as("AWS_REGION").isNotEmpty();
        assertThat(datastoreId).as("HEALTHLAKE_DATASTORE_ID").isNotEmpty();
        assertThat(roleArn).as("HEALTHLAKE_ROLE_ARN").isNotEmpty();
        assertThat(kmsKeyId).as("KMS_KEY_ID").isNotEmpty();

        bucketName = "test-bucket-" + UUID.randomUUID() + "-" + System.currentTimeMillis();

        Consumer<AmazonS3> ifNotExist = (AmazonS3 s3Client) -> {
            if (!s3Client.doesBucketExistV2(bucketName)) {
                System.out.println("Bucket " + bucketName + " does not exist. Creating it now.");
                s3Client.createBucket(new CreateBucketRequest(bucketName, region));
            } else {
                System.out.println("Bucket " + bucketName + " already exists. No action needed.");
            }
        };

        contentStore = new AwsS3ContentStore(bucketName, region, ifNotExist);
        fhirStore = new AwsCliHealthLakeFhirStore(region, datastoreId, bucketName, kmsKeyId, roleArn);

        orchestrator = new HealthInfoOrchestrator()
                .withContentStore(contentStore)
                .withFhirStore(fhirStore);
    }

    @Test
    void testOrchestrateWithBucketCreation() throws IOException {
        final var fhirBundleFile = new File(
                "src/test/resources/synthetic-test-fixtures/fhir-bundles/Adriana394_Stamm704_9a3aa122-31aa-07c3-bcca-47e692653c4c.json");
        final var fhirBundle = VFS.getManager().toFileObject(fhirBundleFile);
        orchestrator.withFhirBundle(fhirBundle).orchestrate();

        final var lastUploadedFileKey = ((AwsS3ContentStore) contentStore).getLastUploadedFileKey();

        assertThat(lastUploadedFileKey).isNotNull();
        assertThat(contentStore.doesFileExistInS3(bucketName, lastUploadedFileKey)).isTrue();

        assertThat(fhirStore.wasExecutionSuccessful()).isTrue();
    }
}