#!/usr/bin/env -S deno run --allow-env --allow-read --allow-net --unstable

import { Command } from "https://deno.land/x/cliffy@v1.0.0-rc.4/command/mod.ts";
import { walk } from "https://deno.land/std@0.224.0/fs/mod.ts";
import { AWSSignerV4 } from "https://deno.land/x/aws_sign_v4@1.0.2/mod.ts";

// Get environment variables
const endpointUrl = Deno.env.get("AWS_HEALTHLAKE_API_ENDPOINT");
const region = Deno.env.get("AWS_REGION");

// Function to directly post JSON data to AWS HealthLake FHIR API using AWSSignerV4
async function postToHealthLake(filePath: string) {
  try {
    const jsonData = await Deno.readTextFile(filePath);

    const request = new Request(`${endpointUrl}/r4/Bundle`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "content-length": jsonData.length.toString(),
      },
      body: jsonData,
    });

    const signer = new AWSSignerV4();
    const signedRequest = await signer.sign("healthlake", request);

    // Send the signed request
    const response = await fetch(signedRequest);

    if (!response.ok) {
      console.error(`Failed to post ${filePath}: ${response.statusText}`);
      const elaboration = await response.text();
      if (elaboration && elaboration.length) {
        console.log(elaboration);
      }
    } else {
      console.log(`Successfully posted ${filePath}`);
    }
  } catch (error) {
    console.error(`Error processing ${filePath}:`, error);
  }
}

// Function to ingest files into AWS HealthLake
async function ingestFiles(path: string, limit: number) {
  try {
    const files = [];
    for await (const entry of walk(path, { exts: [".json"] })) {
      if (files.length >= limit) break;
      if (entry.isFile) {
        files.push(entry.path);
      }
    }

    if (files.length === 0) {
      console.log("No files found to ingest.");
      return;
    }

    for (const file of files) {
      await postToHealthLake(file);
    }
  } catch (error) {
    console.error("Error ingesting files:", error);
  }
}

// Command to check the health and configuration of the AWS setup
const doctorCommand = new Command()
  .description("Check the AWS infrastructure and signing configuration")
  .action(async () => {
    console.log("=== AWS HealthLake Configuration Doctor ===");

    // Check the Endpoint URL
    console.log("HealthLake Endpoint URL:", endpointUrl || "Not set");

    // Check the Region
    console.log("AWS Region:", region || "Not set");

    // Check if the credentials can be resolved
    const accessKeyId = Deno.env.get("AWS_ACCESS_KEY_ID");
    const secretAccessKey = Deno.env.get("AWS_SECRET_ACCESS_KEY");

    if (accessKeyId && secretAccessKey) {
      console.log("AWS Access Key ID:", accessKeyId);
      console.log("AWS Secret Access Key: Set");
    } else {
      console.error("AWS credentials are not set properly.");
      return;
    }

    // Time synchronization check
    try {
      const systemTime = new Date();
      console.log("System Time:", systemTime.toISOString());

      const ntpResponse = await fetch("https://time.google.com/time/now", {
        method: "HEAD",
      });
      const ntpDateHeader = ntpResponse.headers.get("date");
      if (ntpDateHeader) {
        const ntpTime = new Date(ntpDateHeader);
        console.log("NTP Server Time:", ntpTime.toISOString());

        const timeDifference =
          Math.abs(systemTime.getTime() - ntpTime.getTime()) / 1000;
        console.log(`Time Difference: ${timeDifference} seconds`);
        if (timeDifference > 5) {
          console.warn(
            "Significant time difference detected! Consider synchronizing your system clock.",
          );
        } else {
          console.log("Time synchronization is within acceptable limits.");
        }
      } else {
        console.warn(
          "Could not retrieve NTP time. Ensure your system clock is synchronized.",
        );
      }
    } catch (error) {
      console.error("Error checking time synchronization:", error);
    }

    // Attempt to sign a test request and display the signed headers
    try {
      const testJson = JSON.stringify({ test: "value" });

      const request = new Request(`${endpointUrl}/r4/Bundle`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "content-length": testJson.length.toString(),
        },
        body: testJson,
      });

      const signer = new AWSSignerV4();
      const signedRequest = await signer.sign("healthlake", request);

      console.log("Signed Request Headers:", signedRequest.headers);

      // Attempt to send the signed request as a final check
      const response = await fetch(signedRequest);

      if (!response.ok) {
        console.error(`Failed to send test request: ${response.statusText}`);
        const elaboration = await response.text();
        if (elaboration && elaboration.length) {
          console.log(elaboration);
        }
      } else {
        console.log(`Successfully sent test request.`);
      }
    } catch (error) {
      console.error("Error signing request:", error);
    }
  });

// Command to ingest files
const ingestCommand = new Command()
  .description("Ingest JSON files to AWS HealthLake")
  .option("-p, --path <path:string>", "Root directory for JSON files", {
    required: true,
  })
  .option("-l, --limit <limit:number>", "Limit the number of files to ingest", {
    default: 1,
  })
  .action(async ({ path, limit }) => {
    await ingestFiles(path, limit);
  });

// Main command
await new Command()
  .name("hlctl")
  .version("1.0.0")
  .description("HealthLake CLI for ingesting and managing files")
  .command("ingest", ingestCommand)
  .command("doctor", doctorCommand)
  .parse(Deno.args);
