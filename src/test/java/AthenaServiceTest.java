
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import main.java.lib.hios.orch.AthenaService;
import software.amazon.awssdk.regions.Region;

public class AthenaServiceTest {

    private AthenaService athenaService;

    private static final String ATHENA_OUTPUT_BUCKET = "s3://bucketname/";
    private static final String ATHENA_SAMPLE_QUERY = "SELECT * FROM yo";
    private static final long SLEEP_AMOUNT_IN_MS = 1000;
    private static final String ATHENA_DEFAULT_DATABASE = "testdatabase";

    @BeforeEach
    void setUp() {
        athenaService = new AthenaService(Region.US_EAST_2);
    }

    @AfterEach
    void tearDown() {
        athenaService.close();
    }

    @Test
    void testAthenaQuery() {
        // Submit the query
        final var queryExecutionId = athenaService.submitAthenaQuery(
                ATHENA_SAMPLE_QUERY,
                ATHENA_DEFAULT_DATABASE,
                ATHENA_OUTPUT_BUCKET);

        // Validate the query execution ID is not empty
        Assertions.assertThat(queryExecutionId).isNotEmpty();

        // Wait for the query to complete
        try {
            athenaService.waitForQueryToComplete(queryExecutionId, SLEEP_AMOUNT_IN_MS);
        } catch (InterruptedException e) {
            Assertions.fail("Query waiting was interrupted", e);
        }

        // Process the query results
        Assertions.assertThatCode(() -> athenaService.processResultRows(queryExecutionId))
                .doesNotThrowAnyException();
    }
}
