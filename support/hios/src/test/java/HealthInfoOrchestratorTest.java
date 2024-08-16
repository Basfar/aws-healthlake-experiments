import lib.hios.orch.HealthInfoOrchestrator;
import org.apache.commons.vfs2.VFS;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.File;


public class HealthInfoOrchestratorTest {

    @Test
    void testOrchestrateWithAllFileTypes() throws IOException {
        final var orchestrator = new HealthInfoOrchestrator()
                .withContentStore(new HealthInfoOrchestrator.AwsS3ContentStore("",""))
                .withFhirStore(new HealthInfoOrchestrator.AwsHealthLakeFhirStore("",
                        "",
                        "","",""));


        final var fhirBundleFile = new File("");
        final var fhirBundle = VFS.getManager().toFileObject(fhirBundleFile);

        orchestrator.withFhirBundle(fhirBundle);

        orchestrator.orchestrate();
    }






}
