package lib.hios.orch;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.Selectors;
import org.apache.commons.vfs2.VFS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * HealthInfoOrchestrator is responsible for orchestrating the ingestion of FHIR
 * bundles, C-CDA documents,
 * HL7v2 messages, and other FHIR resources into content stores and FHIR stores.
 * The FHIR resources are obtained
 * via Apache Commons VFS {@link FileObject}, allowing them to be sourced from
 * any location, including local file
 * systems, remote servers, cloud storage, or any other file-based repository
 * supported by VFS.
 *
 * The orchestrator is designed for maximum extensibility, enabling the addition
 * of other targets such as Microsoft Azure,
 * Google Cloud, Oracle Cloud, and other cloud providers in the future. This
 * makes the orchestrator flexible and adaptable
 *
 * to evolving healthcare data management needs across different cloud
 * ecosystems.
 * Example usage:
 * 
 * <pre>
 * {@code
 * HealthInfoOrchestrator orchestrator = new HealthInfoOrchestrator()
 *         .withContentStore(new HealthInfoOrchestrator.AwsS3ContentStore())
 *         .withFhirStore(new HealthInfoOrchestrator.AwsHealthLakeFhirStore())
 *         .withFhirBundle(bundle1)
 *         .withFhirBundle(bundle2)
 *         .withFhirBundles(() -> List.of(bundle3, bundle4))
 *         .withCCDA(ccdaFile1)
 *         .withHL7v2(hl7File1);
 * orchestrator.orchestrate();
 * }
 * </pre>
 */

public class HealthInfoOrchestrator {
    private ContentStore contentStore;
    private FhirStore fhirStore;
    private final List<FileObject> bundles = new ArrayList<>();
    private final List<FileObject> ccdaFiles = new ArrayList<>();
    private final List<FileObject> hl7v2Files = new ArrayList<>();

    private static final Logger logger = LoggerFactory.getLogger(HealthInfoOrchestrator.class);

    /**
     * Sets the content store to be used during orchestration.
     *
     * @param contentStore The {@link ContentStore} implementation to use.
     * @return The current instance of {@link HealthInfoOrchestrator} for method
     *         chaining.
     */
    public HealthInfoOrchestrator withContentStore(final ContentStore contentStore) {
        this.contentStore = contentStore;
        return this;
    }

    /**
     * Sets the FHIR store to be used during orchestration.
     *
     * @param fhirStore The {@link FhirStore} implementation to use.
     * @return The current instance of {@link HealthInfoOrchestrator} for method
     *         chaining.
     */
    public HealthInfoOrchestrator withFhirStore(final FhirStore fhirStore) {
        this.fhirStore = fhirStore;
        return this;
    }

    /**
     * Adds a single FHIR bundle to the list of bundles to be processed during
     * orchestration.
     *
     * @param bundle The {@link FileObject} representing the FHIR bundle.
     * @return The current instance of {@link HealthInfoOrchestrator} for method
     *         chaining.
     */
    public HealthInfoOrchestrator withFhirBundle(final FileObject bundle) {
        bundles.add(bundle);
        return this;
    }

    /**
     * Adds multiple FHIR bundles to the list of bundles to be processed during
     * orchestration.
     *
     * @param bundleSupplier A {@link Supplier} providing a list of
     *                       {@link FileObject} instances representing the FHIR
     *                       bundles.
     * @return The current instance of {@link HealthInfoOrchestrator} for method
     *         chaining.
     */
    public HealthInfoOrchestrator withFhirBundles(final Supplier<List<FileObject>> bundleSupplier) {
        final var suppliedBundles = bundleSupplier.get();
        bundles.addAll(suppliedBundles);
        return this;
    }

    /**
     * Adds a single C-CDA document to the list of documents to be processed during
     * orchestration.
     *
     * @param ccdaFile The {@link FileObject} representing the C-CDA document.
     * @return The current instance of {@link HealthInfoOrchestrator} for method
     *         chaining.
     */
    public HealthInfoOrchestrator withCCDA(final FileObject ccdaFile) {
        ccdaFiles.add(ccdaFile);
        return this;
    }

    /**
     * Adds a single HL7v2 message to the list of messages to be processed during
     * orchestration.
     *
     * @param hl7File The {@link FileObject} representing the HL7v2 message.
     * @return The current instance of {@link HealthInfoOrchestrator} for method
     *         chaining.
     */
    public HealthInfoOrchestrator withHL7v2(final FileObject hl7File) {
        hl7v2Files.add(hl7File);
        return this;
    }

    /**
     * Executes the orchestration process, ingesting all previously supplied FHIR
     * bundles, C-CDA documents,
     * and HL7v2 messages into the configured content store and FHIR store.
     *
     * @throws IllegalStateException if either the content store or FHIR store has
     *                               not been set.
     */
    public void orchestrate() throws IOException {
        if (contentStore == null || fhirStore == null) {
            throw new IllegalStateException("ContentStore and FhirStore must be provided before orchestration.");
        }
        logger.info("Orchestrating {} FHIR bundles, {} C-CDA documents, and {} HL7v2 messages", bundles.size(),
                ccdaFiles.size(), hl7v2Files.size());

        for (final var bundle : bundles) {
            logger.info("Processing FHIR bundle: {}", bundle.getName());
            contentStore.store(bundle); // Store FHIR bundle in ContentStore as NDJSON
            fhirStore.execute(String.valueOf(bundle)); // Store FHIR bundle in FHIR Store (e.g., AWS HealthLake) as
        }
        // for (final var ccda : ccdaFiles) {
        // System.out.println("Processing C-CDA document: " + ccda.getName());
        // // Example: Convert C-CDA to FHIR or other format before storing
        // // Example: contentStore.store(ccda);
        // }
        // for (final var hl7 : hl7v2Files) {
        // System.out.println("Processing HL7v2 message: " + hl7.getName());
        // // Example: Convert HL7v2 to FHIR or other format before storing
        // // Example: contentStore.store(hl7);
        //
        // }

    }

    /**
     * Interface representing a generic content store.
     * Concrete implementations should define how to store content.
     */
    public sealed interface ContentStore permits AwsS3ContentStore {
        void store(FileObject file);
    }

    /**
     * AWS S3 implementation of {@link ContentStore}.
     * This class handles the storage of FHIR bundles, C-CDA documents, and HL7v2
     * messages in an AWS S3 bucket.
     * If conversion to formats like .ndjson or other necessary transformations are
     * required before storage,
     * this should be implemented here. This ensures that the data is appropriately
     * formatted for downstream
     * processing and integration.
     */
    public static final class AwsS3ContentStore implements ContentStore {
        /**
         * Stores a file in AWS S3.
         *
         * @param file The {@link FileObject} to be stored.
         */
        private static final Logger logger = LoggerFactory.getLogger(AwsS3ContentStore.class);

        private String lastUploadedFileKey; // Add this field to store the last uploaded file key

        private final String bucketName;
        private final String region;
        private final ObjectMapper objectMapper;
        private final FileSystemManager vfsManager;
        private final AmazonS3 s3Client;

        public AwsS3ContentStore(final String bucketName, final String region) throws FileSystemException {
            this(bucketName, region, null);
        }

        public AwsS3ContentStore(final String bucketName, final String region, final Consumer<AmazonS3> ifNotExist)
                throws FileSystemException {
            this.bucketName = bucketName;
            this.region = region;
            this.objectMapper = new ObjectMapper();
            this.vfsManager = VFS.getManager();
            this.s3Client = AmazonS3ClientBuilder.standard().withRegion(Regions.fromName(region)).build(); // Initialize

            // Check if the bucket exists and create it if not, using the ifNotExist
            // function
            if (!s3Client.doesBucketExistV2(bucketName) && ifNotExist != null) {
                logger.info("Bucket {} does not exist. Executing ifNotExist function.", bucketName);
                ifNotExist.accept(s3Client);
            }
        }

        public void store(final FileObject file) {
            logger.info("Storing file in AWS S3: {}", file.getName());
            // Implement S3 storage logic here, including any necessary format conversion
            try {
                final var ndjson = convertToNdjson(file);
                final var tempFile = createTempFile(ndjson, file.getName().getBaseName());
                lastUploadedFileKey = uploadToS3(tempFile, file.getName().getBaseName());
            } catch (IOException e) {
                logger.error("Error storing file in S3: {}", e.getMessage(), e);
                throw new RuntimeException("Failed to store file in S3", e); // Propagate the exception
            }
        }

        protected String convertToNdjson(final FileObject file) throws IOException {
            try (final var inputStream = file.getContent().getInputStream();
                    final var outputStream = new ByteArrayOutputStream()) {

                final var rootNode = objectMapper.readTree(inputStream);
                if (rootNode.has("entry") && rootNode.get("entry").isArray()) {
                    for (final var entryNode : rootNode.get("entry")) {
                        final var resourceNode = entryNode.get("resource");
                        if (resourceNode != null) {
                            objectMapper.writeValue(outputStream, resourceNode);
                            outputStream.write('\n'); // Add newline after each resource
                        }
                    }
                } else {
                    logger.error("The provided JSON is not a valid FHIR Bundle or does not contain entries.");
                }
                return outputStream.toString(StandardCharsets.UTF_8);
            }
        }

        private FileObject createTempFile(final String content, final String fileName)
                throws IOException, FileSystemException {
            final var tempFile = File.createTempFile(UUID.randomUUID().toString(), ".ndjson");
            try (final var fileWriter = new FileWriter(tempFile)) {
                fileWriter.write(content);
            }
            return vfsManager.resolveFile(tempFile.getAbsolutePath());
        }

        private String uploadToS3(final FileObject srcFile, final String fileName) throws FileSystemException {
            final var destFileKey = UUID.randomUUID().toString() + "_" + fileName + ".ndjson";
            final var destFileUri = "s3://s3-" + region + ".amazonaws.com/" + bucketName + "/" + destFileKey;
            final var destFile = vfsManager.resolveFile(destFileUri);

            final var destFolder = vfsManager.resolveFile(destFile.getParent().getName().getURI());
            if (!destFolder.exists()) {
                destFolder.createFolder();
            }
            destFile.copyFrom(srcFile, Selectors.SELECT_SELF);
            logger.info("Successfully uploaded {} to S3 bucket {}", destFileKey, bucketName);

            return destFileKey; // Return the key of the uploaded file
        }

        public String getLastUploadedFileKey() {
            return lastUploadedFileKey;
        }

        public boolean isEmptyFile(FileObject fileObject) throws IOException {
            try (InputStream inputStream = fileObject.getContent().getInputStream()) {
                return inputStream.available() == 0;
            }
        }

        public boolean doesFileExistInS3(String bucketName, String objectKey) {
            try (S3Client s3Client = S3Client.builder().region(Region.US_EAST_2).build()) {
                HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                        .bucket(bucketName)
                        .key(objectKey)
                        .build();

                try {
                    s3Client.headObject(headObjectRequest); // This line should work now
                    return true; // Object exists if no exception is thrown
                } catch (NoSuchKeyException e) {
                    return false; // Object doesn't exist
                }
            }
        }
    }

    /**
     * Interface representing a generic FHIR store.
     * Concrete implementations should define how to store FHIR data.
     */
    public sealed interface FhirStore permits AwsCliHealthLakeFhirStore {
        void execute(final String file);
    }

    /**
     * AWS HealthLake implementation of {@link FhirStore}.
     * This class handles the storage of FHIR bundles in AWS HealthLake.
     *
     * <p>
     * <b>Note:</b> Initially, we aimed to use the AWS SDK to start a FHIR import
     * job directly.
     * We tried to use the following code:
     * 
     * <pre>
     * final var importJobRequest = StartFHIRImportJobRequest.builder()
     *         .datastoreId(datastoreId)
     *         .inputDataConfig(inputDataConfig)
     *         .jobName("FHIRImportJob_" + System.currentTimeMillis())
     *         .dataAccessRoleArn("arn:aws:iam::992382710407:role/service-role/AWSHealthLake-Import-ServiceRole")
     *         .build();
     * </pre>
     * 
     * However, we encountered issues with resolving the `StartFHIRImportJobRequest`
     * class from the AWS SDK.
     * Due to this, we have resorted to using the AWS CLI through the
     * {@link AwsCliInvoker} to execute the FHIR import job.
     *
     * <p>
     * The {@link AwsCliHealthLakeFhirStore} class provides methods to upload files
     * to
     * AWS S3 and then start a FHIR import job in AWS HealthLake.
     * It uses the AWS CLI to perform the import job because the AWS SDK's
     * functionality for this operation could not be utilized directly.
     *
     * <p>
     * The {@link AwsCliHealthLakeFhirStore#execute(String)} method performs the
     * following:
     * <ol>
     * <li>Retrieves the latest file from the S3 bucket with a `.ndjson`
     * extension.</li>
     * <li>Constructs the S3 URI for the input file and the output location.</li>
     * <li>Constructs an AWS CLI command to start the FHIR import job in
     * HealthLake.</li>
     * <li>Uses the {@link AwsCliInvoker} to execute the AWS CLI command and start
     * the import job.</li>
     * </ol>
     */
    public static final class AwsCliHealthLakeFhirStore implements FhirStore {
        /**
         * Stores a FHIR bundle in AWS HealthLake.
         *
         * @param file The {@link FileObject} representing the FHIR bundle to be stored.
         */

        private static final Logger logger = LoggerFactory.getLogger(AwsCliHealthLakeFhirStore.class);

        private boolean executionSuccessful = false; // Add this field

        private String datastoreId;
        private final String bucketName;
        private final String roleArn;
        private final Region region;
        private final String kmsKeyId;
        private final S3Client s3Client;

        public AwsCliHealthLakeFhirStore(final String region, final String datastoreId, final String bucketName,
                final String kmsKeyId, final String roleArn) {
            this.datastoreId = datastoreId;
            this.bucketName = bucketName;
            this.roleArn = roleArn;
            this.region = Region.of(region);
            this.kmsKeyId = kmsKeyId;
            this.s3Client = S3Client.builder().region(this.region).build();
        }

        public void execute(final String fileName) {
            final var s3ObjectKey = getLatestS3ObjectKey();
            if (s3ObjectKey == null) {
                logger.error("No objects found in the S3 bucket.");
                executionSuccessful = false;
                throw new RuntimeException("No objects found in the S3 bucket."); // Propagate the error
            }
            final var s3Uri = constructS3Uri(bucketName, s3ObjectKey);
            final var outputS3Uri = constructS3Uri(bucketName, "output");

            logger.info("Starting HealthLake import for file: {}", s3Uri);
            final var command = String.format(
                    "aws healthlake start-fhir-import-job " +
                            "--input-data-config S3Uri=%s " +
                            "--datastore-id %s " +
                            "--data-access-role-arn \"%s\" " +
                            "--job-output-data-config '{\"S3Configuration\": {\"S3Uri\":\"%s\",\"KmsKeyId\":\"%s\"}}' "
                            +
                            "--region %s",
                    s3Uri,
                    datastoreId,
                    roleArn,
                    outputS3Uri,
                    kmsKeyId,
                    region);
            try {
                final var cliInvoker = new AwsCliInvoker();
                final var result = cliInvoker.executeAwsCliCommand(command);
                logger.info("Command output:\n{}", result);
                executionSuccessful = true;
            } catch (IOException | InterruptedException e) {
                logger.error("Error executing AWS CLI command: {}", e.getMessage(), e);
                executionSuccessful = false;
                throw new RuntimeException("Failed to execute AWS CLI command", e);
            }
        }

        public boolean wasExecutionSuccessful() {
            return executionSuccessful;
        }

        public String getLatestS3ObjectKey() {
            final var listObjectsRequest = ListObjectsV2Request.builder()
                    .bucket(bucketName)
                    .build();

            final var result = s3Client.listObjectsV2(listObjectsRequest);

            return result.contents().stream()
                    .filter(os -> os.key().endsWith(".ndjson"))
                    .max(Comparator.comparing(S3Object::lastModified))
                    .map(S3Object::key)
                    .orElse(null);
        }

        private String constructS3Uri(final String bucket, final String key) {
            return String.format("s3://%s/%s", bucket, key);
        }
    }
}