## Prepare HealthLake environment

- [Create your HealthLake Data Store](https://us-east-2.console.aws.amazon.com/healthlake/home)
- Use your Data Store ID to get your API endpoint -- this is the
  `AWS_HEALTHLAKE_API_ENDPOINT`
- Use [IAM Console](https://console.aws.amazon.com/iam) to get your API endpoint
  credentials
  - e.g. `aws-healthlake-experimental-api-user` -- this is the
    `AWS_HEALTHLAKE_API_ENDPOINT_USERID`
  - Attach `AmazonHealthLakeFullAccess` policies directly to this user and use
    Access keys (for experimental synthetic data you can use _Application
    running outside AWS_ keys but use _IAM Roles Anywhere_ for production)
