import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as iam from "aws-cdk-lib/aws-iam";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as kms from "aws-cdk-lib/aws-kms";
import * as healthlake from "aws-cdk-lib/aws-healthlake";
import * as secretsmanager from "aws-cdk-lib/aws-secretsmanager";

interface ExperimentalStackProps extends cdk.StackProps {
  userName: string;
  bucketName: string;
  healthLakeDatastoreName: string;
}

class ExperimentalStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: ExperimentalStackProps) {
    super(scope, id, props);

    const { userName, bucketName, healthLakeDatastoreName } = props;

    // Create an S3 bucket with default encryption
    const bucket = new s3.Bucket(this, "ExperimentalBucket", {
      bucketName,
      encryption: s3.BucketEncryption.S3_MANAGED,
      removalPolicy: cdk.RemovalPolicy.DESTROY, // Only for experimental use
      autoDeleteObjects: true, // Only for experimental use
    });

    // Create a KMS key for HealthLake
    const kmsKey = new kms.Key(this, "HealthLakeKey", {
      enableKeyRotation: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY, // Only for experimental use
    });

    // Create an IAM user for developers
    const user = new iam.User(this, "ExperimentalUser", {
      userName,
    });

    // Attach a policy to the user for S3 access
    const s3AccessPolicy = new iam.Policy(this, "S3AccessPolicy", {
      statements: [
        new iam.PolicyStatement({
          actions: ["s3:*"],
          resources: [bucket.bucketArn, `${bucket.bucketArn}/*`],
        }),
      ],
    });

    user.attachInlinePolicy(s3AccessPolicy);

    // Create access keys for the user and store them in Secrets Manager
    const accessKey = new iam.CfnAccessKey(this, "UserAccessKey", {
      userName: user.userName,
    });

    new secretsmanager.Secret(this, "UserApiKeySecret", {
      secretStringValue: cdk.SecretValue.secretsManagerSecretArn({
        arn: accessKey.attrSecretAccessKey,
      }),
    });

    // Create an IAM role for HealthLake with permissions to access the S3 bucket
    const healthLakeRole = new iam.Role(this, "HealthLakeRole", {
      assumedBy: new iam.ServicePrincipal("healthlake.amazonaws.com"),
      inlinePolicies: {
        HealthLakeS3Access: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              actions: ["s3:GetObject", "s3:ListBucket"],
              resources: [bucket.bucketArn, `${bucket.bucketArn}/*`],
            }),
            new iam.PolicyStatement({
              actions: ["kms:Decrypt"],
              resources: [kmsKey.keyArn],
            }),
          ],
        }),
      },
    });

    // Create a HealthLake datastore
    const datastore = new healthlake.CfnFHIRDatastore(
      this,
      "HealthLakeDatastore",
      {
        datastoreName: healthLakeDatastoreName,
        datastoreTypeVersion: "R4",
        sseConfiguration: {
          kmsEncryptionConfig: {
            cmkType: "CUSTOMER_MANAGED_KMS_KEY",
            kmsKeyId: kmsKey.keyArn,
          },
        },
        dataAccessRoleArn: healthLakeRole.roleArn,
      },
    );

    // Output the user credentials and datastore information
    new cdk.CfnOutput(this, "UserAccessKeyId", {
      value: accessKey.ref,
    });

    new cdk.CfnOutput(this, "UserSecretAccessKey", {
      value: accessKey.attrSecretAccessKey,
    });

    new cdk.CfnOutput(this, "S3BucketName", {
      value: bucket.bucketName,
    });

    new cdk.CfnOutput(this, "HealthLakeDatastoreId", {
      value: datastore.attrDatastoreId,
    });
  }
}

const app = new cdk.App();
new ExperimentalStack(app, "ExperimentalStack", {
  userName: "experimental-user",
  bucketName: "health-info-orch-service-experimental",
  healthLakeDatastoreName: "experimental-datastore",
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION,
  },
});
