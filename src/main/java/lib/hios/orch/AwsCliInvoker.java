package lib.hios.orch;

import java.io.BufferedReader;

import java.io.InputStreamReader;

import java.io.IOException;



/**
 * Utility class for executing AWS CLI commands from within Java applications.
 *
 * <p>This class is designed to be used as a last resort when the AWS Java SDK fails to perform required operations.
 * For instance, if the AWS Java SDK method to start a HealthLake job does not work, this class can be used to invoke
 * the AWS CLI directly to achieve the same result.</p>
 *
 * <p>The `AwsCliInvoker` class provides a method to execute AWS CLI commands and retrieve their output. It wraps
 * the execution of shell commands using the `ProcessBuilder` class and captures the command's output.</p>
 *
 * <p>Note that using this class requires the AWS CLI to be installed and configured on the machine where the code
 * is executed. Also, proper handling of command line escaping and security considerations should be taken into
 * account when constructing commands.</p>
 *
 * <p>Example usage:</p>
 * <pre>
 * {@code
 * AwsCliInvoker cliInvoker = new AwsCliInvoker();
 * try {
 *     String command = "aws healthlake start-fhir-import-job " +
 *                      "--input-data-config S3Uri=s3://mybucket/myfile.json " +
 *                      "--datastore-id my-datastore-id " +
 *                      "--data-access-role-arn arn:aws:iam::123456789012:role/my-role " +
 *                      "--job-output-data-config '{\"S3Configuration\": {\"S3Uri\":\"s3://mybucket/output\",\"KmsKeyId\":\"arn:aws:kms:region:123456789012:key/my-key-id\"}}' " +
 *                      "--region my-region";
 *     String result = cliInvoker.executeAwsCliCommand(command);
 *     System.out.println("Command output:\n" + result);
 * } catch (IOException | InterruptedException e) {
 *     e.printStackTrace();
 * }
 * }
 * </pre>
 */

public class AwsCliInvoker {

    /**
     * Executes the specified AWS CLI command and returns its output.
     *
     * <p>This method constructs a `ProcessBuilder` to execute the given command in a new shell process. It reads the
     * command's output from the process's input stream and waits for the process to complete. If the command exits
     * with a non-zero exit code, a {@link RuntimeException} is thrown.</p>
     *
     * @param command The AWS CLI command to execute.
     * @return The output of the command as a {@link String}.
     * @throws IOException If an I/O error occurs while executing the command or reading the output.
     * @throws InterruptedException If the current thread is interrupted while waiting for the process to complete.
     */

    public String executeAwsCliCommand(final String command) throws IOException, InterruptedException {
        final var processBuilder = new ProcessBuilder();
        processBuilder.command("bash", "-c", command);

        final var process = processBuilder.start();

        final var output = new StringBuilder();

        try (final var reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line).append("\n");
            }
        }

        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new RuntimeException("Failed to execute command: " + command + " with exit code " + exitCode);
        }

        return output.toString().trim();
    }

    /**
     * Main method for testing the `AwsCliInvoker` class.
     *
     * <p>This method demonstrates how to use the `AwsCliInvoker` to execute an AWS CLI command that starts an AWS
     * HealthLake import job. The command and its parameters are hard-coded in the example. The result of the command
     * execution is printed to the standard output.</p>
     *
     * @param args Command-line arguments (not used).
     */
    public static void main(String[] args) {
        final var cliInvoker = new AwsCliInvoker();

        try {
            // Define the AWS CLI command with proper escaping
            String command = "aws healthlake start-fhir-import-job " +
                    "--input-data-config S3Uri=s3://javahios/1aa55222-e73f-40c2-bd3e-7f1694afa9d0_Adam631_Orn563_cfe5776e-1184-1359-b040-1088c27ed1b5.json.ndjson " +
                    "--datastore-id 5e89353b17a720c1aa6a6c66a02e880c " +
                    "--data-access-role-arn \"arn:aws:iam::992382710407:role/service-role/AWSHealthLake-Import-ServiceRole\" " +
                    "--job-output-data-config '{\"S3Configuration\": {\"S3Uri\":\"s3://javahios/healthlake-output\",\"KmsKeyId\":\"arn:aws:kms:us-east-2:992382710407:key/4faf8e6c-ce8a-45a8-8e81-dcb7c662fefb\"}}' " +
                    "--region us-east-2";

            String result = cliInvoker.executeAwsCliCommand(command);

            System.out.println("Command output:\n" + result);

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}