import lib.hios.orch.HealthInfoOrchestrator;
import org.apache.commons.vfs2.VFS;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.File;


public class HealthInfoOrchestratorTest {

    @Test
    void testOrchestrateWithAllFileTypes() throws IOException {
        final var orchestrator = new HealthInfoOrchestrator()
                .withContentStore(new HealthInfoOrchestrator.AwsS3ContentStore("javahios","us-east-2"))
                .withFhirStore(new HealthInfoOrchestrator.AwsHealthLakeFhirStore("us-east-2",
                        "5e89353b17a720c1aa6a6c66a02e880c",
                        "javahios","arn:aws:kms:us-east-2:992382710407:key/4faf8e6c-ce8a-45a8-8e81-dcb7c662fefb","arn:aws:iam::992382710407:role/service-role/AWSHealthLake-Import-ServiceRole"));


        final var fhirBundleFile = new File("src/test/resources/synthea_sample_data_fhir_latest/Ángela136_Cortez851_d2e56049-1c7c-45ac-5d87-1b81dc9db112.json");
        final var fhirBundle = VFS.getManager().toFileObject(fhirBundleFile);

        orchestrator.withFhirBundle(fhirBundle);

        orchestrator.orchestrate();
    }






}
