package lib.hios.orch;


import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.AmazonS3;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.Selectors;
import java.io.*;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;


import java.util.UUID;
import java.util.function.Supplier;


/**
 * HealthInfoOrchestrator is responsible for orchestrating the ingestion of FHIR bundles, C-CDA documents,
 * HL7v2 messages, and other FHIR resources into content stores and FHIR stores. The FHIR resources are obtained
 * via Apache Commons VFS {@link FileObject}, allowing them to be sourced from any location, including local file
 * systems, remote servers, cloud storage, or any other file-based repository supported by VFS.
 *
 * The orchestrator is designed for maximum extensibility, enabling the addition of other targets such as Microsoft Azure,
 * Google Cloud, Oracle Cloud, and other cloud providers in the future. This makes the orchestrator flexible and adaptable
 *
 * to evolving healthcare data management needs across different cloud ecosystems.
 * Example usage:
 * <pre>
 * {@code
 * HealthInfoOrchestrator orchestrator = new HealthInfoOrchestrator()
 *     .withContentStore(new HealthInfoOrchestrator.AwsS3ContentStore())
 *     .withFhirStore(new HealthInfoOrchestrator.AwsHealthLakeFhirStore())
 *     .withFhirBundle(bundle1)
 *     .withFhirBundle(bundle2)
 *     .withFhirBundles(() -> List.of(bundle3, bundle4))
 *     .withCCDA(ccdaFile1)
 *     .withHL7v2(hl7File1);
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



    /**å
     * Sets the content store to be used during orchestration.
     *
     * @param contentStore The {@link ContentStore} implementation to use.
     * @return The current instance of {@link HealthInfoOrchestrator} for method chaining.
     */

    public HealthInfoOrchestrator withContentStore(final ContentStore contentStore) {
        this.contentStore = contentStore;
        return this;
    }


    /**
     * Sets the FHIR store to be used during orchestration.
     *
     * @param fhirStore The {@link FhirStore} implementation to use.
     * @return The current instance of {@link HealthInfoOrchestrator} for method chaining.
     */

    public HealthInfoOrchestrator withFhirStore(final FhirStore fhirStore) {
        this.fhirStore = fhirStore;
        return this;
    }


    /**
     * Adds a single FHIR bundle to the list of bundles to be processed during orchestration.
     *
     * @param bundle The {@link FileObject} representing the FHIR bundle.
     * @return The current instance of {@link HealthInfoOrchestrator} for method chaining.
     */

    public HealthInfoOrchestrator withFhirBundle(final FileObject bundle) {
        bundles.add(bundle);
        return this;
    }


    /**
     * Adds multiple FHIR bundles to the list of bundles to be processed during orchestration.
     *
     * @param bundleSupplier A {@link Supplier} providing a list of {@link FileObject} instances representing the FHIR bundles.
     * @return The current instance of {@link HealthInfoOrchestrator} for method chaining.
     */

    public HealthInfoOrchestrator withFhirBundles(final Supplier<List<FileObject>> bundleSupplier) {
        final var suppliedBundles = bundleSupplier.get();
        bundles.addAll(suppliedBundles);
        return this;
    }


    /**
     * Adds a single C-CDA document to the list of documents to be processed during orchestration.
     *
     * @param ccdaFile The {@link FileObject} representing the C-CDA document.
     * @return The current instance of {@link HealthInfoOrchestrator} for method chaining.
     */

    public HealthInfoOrchestrator withCCDA(final FileObject ccdaFile) {
        ccdaFiles.add(ccdaFile);
        return this;
    }


    /**
     * Adds a single HL7v2 message to the list of messages to be processed during orchestration.
     *
     * @param hl7File The {@link FileObject} representing the HL7v2 message.
     * @return The current instance of {@link HealthInfoOrchestrator} for method chaining.
     */

    public HealthInfoOrchestrator withHL7v2(final FileObject hl7File) {
        hl7v2Files.add(hl7File);
        return this;
    }



    /**
     * Executes the orchestration process, ingesting all previously supplied FHIR bundles, C-CDA documents,
     * and HL7v2 messages into the configured content store and FHIR store.
     *
     * @throws IllegalStateException if either the content store or FHIR store has not been set.
     */

    public void orchestrate() {
        if (contentStore == null || fhirStore == null) {
            throw new IllegalStateException("ContentStore and FhirStore must be provided before orchestration.");
        }
        System.out.println("Orchestrating " + bundles.size() + " FHIR bundles, " + ccdaFiles.size() + " C-CDA documents, and " + hl7v2Files.size() + " HL7v2 messages");

        for (final var bundle : bundles) {
            System.out.println("Processing FHIR bundle: " + bundle.getName());
            // Example: contentStore.store(bundle);
            contentStore.store(bundle);  // Store FHIR bundle in ContentStore as NDJSON
            fhirStore.execute(String.valueOf(bundle));     // Store FHIR bundle in FHIR Store (e.g., AWS HealthLake) as NDJSON
            // Example: fhirStore.store(bundle);
        }
//        for (final var ccda : ccdaFiles) {
//            System.out.println("Processing C-CDA document: " + ccda.getName());
//            // Example: Convert C-CDA to FHIR or other format before storing
//            // Example: contentStore.store(ccda);
//        }
//        for (final var hl7 : hl7v2Files) {
//            System.out.println("Processing HL7v2 message: " + hl7.getName());
//            // Example: Convert HL7v2 to FHIR or other format before storing
//            // Example: contentStore.store(hl7);
//
//        }

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
     * This class handles the storage of FHIR bundles, C-CDA documents, and HL7v2 messages in an AWS S3 bucket.
     * If conversion to formats like .ndjson or other necessary transformations are required before storage,
     * this should be implemented here. This ensures that the data is appropriately formatted for downstream
     * processing and integration.
     */
    public static final class AwsS3ContentStore implements ContentStore {

        /**
         * Stores a file in AWS S3.
         *
         * @param file The {@link FileObject} to be stored.
         */

        private final String bucketName;

        private final String region;
        private final ObjectMapper objectMapper;
        private final FileSystemManager vfsManager;


        public AwsS3ContentStore(final String bucketName, final String region) throws FileSystemException {
            this.bucketName = bucketName;
            this.region = region;
            this.objectMapper = new ObjectMapper();
            this.vfsManager = VFS.getManager();
        }

        public void store(final FileObject file) {
            System.out.println("Storing file in AWS S3: " + file.getName());
            // Implement S3 storage logic here, including any necessary format conversion
            try {
                final var ndjson = convertToNdjson(file);
                final var tempFile = createTempFile(ndjson, file.getName().getBaseName());
                uploadToS3(tempFile, file.getName().getBaseName());
            } catch (IOException e) {
                System.err.println("Error storing file in S3: " + e.getMessage());
                e.printStackTrace();
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
                    System.err.println("The provided JSON is not a valid FHIR Bundle or does not contain entries.");
                }

                return outputStream.toString(StandardCharsets.UTF_8);
            }

        }

        private FileObject createTempFile(final String content, final String fileName) throws IOException, FileSystemException {
            final var tempFile = File.createTempFile(UUID.randomUUID().toString(), ".ndjson");
            try (final var fileWriter = new FileWriter(tempFile)) {
                fileWriter.write(content);
            }
            return vfsManager.resolveFile(tempFile.getAbsolutePath());
        }

        private void uploadToS3(final FileObject srcFile, final String fileName) throws FileSystemException {
            final var destFileUri = "s3://s3-"+region+".amazonaws.com/" + bucketName + "/" + UUID.randomUUID().toString() + "_" + fileName + ".ndjson";
            final var destFile = vfsManager.resolveFile(destFileUri);

            final var destFolder = vfsManager.resolveFile(destFile.getParent().getName().getURI());
            if (!destFolder.exists()) {
                destFolder.createFolder();
            }

            destFile.copyFrom(srcFile, Selectors.SELECT_SELF);

            System.out.println("Successfully uploaded " + fileName + " to S3 bucket " + bucketName);
        }



    }


    /**
     * Interface representing a generic FHIR store.
     * Concrete implementations should define how to store FHIR data.
     */

    public sealed interface FhirStore permits AwsHealthLakeFhirStore {
        // Define methods for storing FHIR data
        void execute(final String file);
    }


    /**
     * AWS HealthLake implementation of {@link FhirStore}.
     * This class handles the storage of FHIR bundles in AWS HealthLake.
     *
     * <p><b>Note:</b> Initially, we aimed to use the AWS SDK to start a FHIR import job directly.
     * We tried to use the following code:
     * <pre>
     * final var importJobRequest = StartFHIRImportJobRequest.builder()
     *         .datastoreId(datastoreId)
     *         .inputDataConfig(inputDataConfig)
     *         .jobName("FHIRImportJob_" + System.currentTimeMillis())
     *         .dataAccessRoleArn("arn:aws:iam::992382710407:role/service-role/AWSHealthLake-Import-ServiceRole")
     *         .build();
     * </pre>
     * However, we encountered issues with resolving the `StartFHIRImportJobRequest` class from the AWS SDK.
     * Due to this, we have resorted to using the AWS CLI through the {@link AwsCliInvoker} to execute the FHIR import job.
     *
     * <p>The {@link AwsHealthLakeFhirStore} class provides methods to upload files to AWS S3 and then start a FHIR import job in AWS HealthLake.
     * It uses the AWS CLI to perform the import job because the AWS SDK's functionality for this operation could not be utilized directly.
     *
     * <p>The {@link AwsHealthLakeFhirStore#execute(String)} method performs the following:
     * <ol>
     *     <li>Retrieves the latest file from the S3 bucket with a `.ndjson` extension.</li>
     *     <li>Constructs the S3 URI for the input file and the output location.</li>
     *     <li>Constructs an AWS CLI command to start the FHIR import job in HealthLake.</li>
     *     <li>Uses the {@link AwsCliInvoker} to execute the AWS CLI command and start the import job.</li>
     * </ol>
     */


    public static final class AwsHealthLakeFhirStore implements FhirStore {
        /**
         * Stores a FHIR bundle in AWS HealthLake.
         *
         * @param file The {@link FileObject} representing the FHIR bundle to be stored.
         */
        private final String datastoreId;
        private final String bucketName;
        private final String roleArn;
        private final String region;
        private final String kmsKeyId;
        private final AmazonS3 s3Client;

        public AwsHealthLakeFhirStore(final String region, final String datastoreId, final String bucketName,final String kmsKeyId,final String roleArn) {
            this.datastoreId = datastoreId;
            this.bucketName = bucketName;
            this.roleArn = roleArn;
            this.region = region;
            this.kmsKeyId = kmsKeyId;

            this.s3Client = AmazonS3ClientBuilder.standard().withRegion(Regions.fromName(region)).build();
        }

        public void execute(final String fileName) {

            final var s3ObjectKey = getLatestS3ObjectKey();
            if (s3ObjectKey == null) {
                System.err.println("No objects found in the S3 bucket.");
                return;
            }
            final var s3Uri = constructS3Uri(bucketName, s3ObjectKey);
            final var outputS3Uri = constructS3Uri(bucketName, "output");

            System.out.println("Starting HealthLake import for file: " + s3Uri);
            final var command = String.format(
                    "aws healthlake start-fhir-import-job " +
                            "--input-data-config S3Uri=%s " +
                            "--datastore-id %s " +
                            "--data-access-role-arn \"%s\" " +
                            "--job-output-data-config '{\"S3Configuration\": {\"S3Uri\":\"%s\",\"KmsKeyId\":\"%s\"}}' " +
                            "--region %s",
                    s3Uri,
                    datastoreId,
                    roleArn,
                    outputS3Uri,
                    kmsKeyId,
                    region
            );
            try {
                final var cliInvoker = new AwsCliInvoker();
                final var result = cliInvoker.executeAwsCliCommand(command);
                System.out.println("Command output:\n" + result);
            } catch (IOException | InterruptedException e) {
                System.err.println("Error executing AWS CLI command: " + e.getMessage());
                e.printStackTrace();
            }
        }

        private String getLatestS3ObjectKey() {
            final var result = s3Client.listObjectsV2(bucketName);
            return result.getObjectSummaries().stream()
                    .filter(os -> os.getKey().endsWith(".ndjson"))
                    .max(Comparator.comparing(S3ObjectSummary::getLastModified))
                    .map(S3ObjectSummary::getKey)
                    .orElse(null);
        }


        private String constructS3Uri(final String bucket,final String key) {
            return String.format("s3://%s/%s", bucket, key);
        }

    }

}