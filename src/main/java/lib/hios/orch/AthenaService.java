package main.java.lib.hios.orch;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.*;
import software.amazon.awssdk.services.athena.paginators.GetQueryResultsIterable;

import java.util.List;

public class AthenaService {

    private final AthenaClient athenaClient;

    public AthenaService(final Region region) {
        this.athenaClient = AthenaClient.builder()
                .region(region)
                .build();
    }

    public String submitAthenaQuery(final String query, final String database, final String outputBucket) {
        try {
            final var queryExecutionContext = QueryExecutionContext.builder()
                    .database(database)
                    .build();

            final var resultConfiguration = ResultConfiguration.builder()
                    .outputLocation(outputBucket)
                    .build();

            final var startQueryExecutionRequest = StartQueryExecutionRequest.builder()
                    .queryString(query)
                    .queryExecutionContext(queryExecutionContext)
                    .resultConfiguration(resultConfiguration)
                    .build();

            final var startQueryExecutionResponse = athenaClient
                    .startQueryExecution(startQueryExecutionRequest);
            return startQueryExecutionResponse.queryExecutionId();

        } catch (AthenaException e) {
            e.printStackTrace();
            throw new RuntimeException("Error submitting Athena query", e);
        }
    }

    public void waitForQueryToComplete(final String queryExecutionId, final long sleepAmountInMs)
            throws InterruptedException {
        final var getQueryExecutionRequest = GetQueryExecutionRequest.builder()
                .queryExecutionId(queryExecutionId)
                .build();

        GetQueryExecutionResponse getQueryExecutionResponse;
        var isQueryStillRunning = true;
        while (isQueryStillRunning) {
            getQueryExecutionResponse = athenaClient.getQueryExecution(getQueryExecutionRequest);
            final var queryState = getQueryExecutionResponse.queryExecution().status().state().toString();
            if (queryState.equals(QueryExecutionState.FAILED.toString())) {
                throw new RuntimeException("The Amazon Athena query failed to run with error message: " +
                        getQueryExecutionResponse.queryExecution().status().stateChangeReason());
            } else if (queryState.equals(QueryExecutionState.CANCELLED.toString())) {
                throw new RuntimeException("The Amazon Athena query was cancelled.");
            } else if (queryState.equals(QueryExecutionState.SUCCEEDED.toString())) {
                isQueryStillRunning = false;
            } else {
                Thread.sleep(sleepAmountInMs);
            }
            System.out.println("The current status is: " + queryState);
        }
    }

    public void processResultRows(final String queryExecutionId) {
        try {
            final var getQueryResultsRequest = GetQueryResultsRequest.builder()
                    .queryExecutionId(queryExecutionId)
                    .build();

            GetQueryResultsIterable getQueryResultsResults = athenaClient
                    .getQueryResultsPaginator(getQueryResultsRequest);
            for (final var result : getQueryResultsResults) {
                final var columnInfoList = result.resultSet().resultSetMetadata().columnInfo();
                final var results = result.resultSet().rows();
                processRow(results, columnInfoList);
            }

        } catch (AthenaException e) {
            e.printStackTrace();
            throw new RuntimeException("Error processing Athena query results", e);
        }
    }

    private void processRow(final List<Row> row, final List<ColumnInfo> columnInfoList) {
        for (final var myRow : row) {
            List<Datum> allData = myRow.data();
            for (final var data : allData) {
                System.out.println("The value of the column is " + data.varCharValue());
            }
        }
    }

    public void close() {
        athenaClient.close();
    }
}
