#!/usr/bin/env python

"""
hiosctl.py - A script for storing FHIR bundles in S3, ingesting them into AWS HealthLake, and verifying environment setup.

Preparation:
1. Install the boto3 library:
   $ pip install boto3

2. Set the following environment variables with your AWS credentials and region:
   $ export AWS_ACCESS_KEY_ID='your-access-key-id'
   $ export AWS_SECRET_ACCESS_KEY='your-secret-access-key'
   $ export AWS_REGION='your-aws-region'

Usage:
1. Store in S3:
   $ ./hiosctl.py store-s3 --bucket-name your-bucket-name --path /path/to/synthea_fhir_bundles

2. Ingest into HealthLake:
   $ ./hiosctl.py ingest-healthlake --bucket-name your-bucket-name --datastore-id your-datastore-id --darole-arn your-darole-arn

3. Verify environment and permissions:
   $ ./hiosctl.py doctor --bucket-name your-bucket-name --datastore-id your-datastore-id
"""

import os
import boto3
import sys
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError

def upload_to_s3(s3_client, file_path, bucket_name, object_name):
    try:
        s3_client.head_object(Bucket=bucket_name, Key=object_name)
        print(f"File already exists in s3://{bucket_name}/{object_name}, skipping upload.")
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            try:
                s3_client.upload_file(file_path, bucket_name, object_name, ExtraArgs={'ContentType': 'application/json'})
                print(f"File uploaded successfully to s3://{bucket_name}/{object_name}")
            except FileNotFoundError:
                print(f"The file {file_path} was not found.")
            except NoCredentialsError:
                print("Credentials not available.")
            except PartialCredentialsError:
                print("Incomplete credentials provided.")
            except Exception as e:
                print(f"An error occurred while uploading {file_path}: {e}")
        else:
            print(f"An error occurred: {e}")

def store_s3(bucket_name, synthea_fhir_bundles):
    session = boto3.Session(
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_REGION')
    )
    s3_client = session.client('s3')

    for root, dirs, files in os.walk(synthea_fhir_bundles):
        for filename in files:
            if filename.endswith('.json'):
                file_path = os.path.join(root, filename)
                object_name = os.path.relpath(file_path, synthea_fhir_bundles).replace("\\", "/")
                upload_to_s3(s3_client, file_path, bucket_name, object_name)

def ingest_healthlake(bucket_name, datastore_id, data_access_role_arn):    
    session = boto3.Session(
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_REGION')
    )
    s3_client = session.client('s3')
    healthlake_client = session.client('healthlake')

    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in response:
            for item in response['Contents']:
                object_name = item['Key']
                s3_uri = f's3://{bucket_name}/{object_name}'
                print(f"Ingesting {s3_uri} into AWS HealthLake...")

                try:
                    healthlake_response = healthlake_client.start_fhir_import_job(
                        DatastoreId=datastore_id,
                        InputDataConfig={
                            'S3Uri': s3_uri
                        },
                        JobName=f'Ingest-{object_name[:55]}',  # Ensure job name is within the 64-character limit
                        DataAccessRoleArn=data_access_role_arn,
                        JobOutputDataConfig={
                            'S3Configuration': {
                                'S3Uri': f's3://{bucket_name}/healthlake-start_fhir_import_job-output/',
                                'KmsKeyId': '' # TODO: need to attach through CLI argument
                            }
                        }
                    )
                    print(f"Ingestion started for {s3_uri}. Job ID: {healthlake_response['JobId']}")
                except ClientError as e:
                    print(f"Failed to ingest {s3_uri}: {e}")
        else:
            print("No objects found in the S3 bucket.")
    except NoCredentialsError:
        print("Credentials not available.")
    except PartialCredentialsError:
        print("Incomplete credentials provided.")
    except Exception as e:
        print(f"An error occurred: {e}")

def doctor(bucket_name, datastore_id):
    required_env_vars = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_REGION']
    missing_env_vars = [var for var in required_env_vars if os.getenv(var) is None]

    if missing_env_vars:
        print(f"Missing required environment variables: {', '.join(missing_env_vars)}")
        sys.exit(1)
    else:
        print("All required environment variables are set.")

    session = boto3.Session(
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_REGION')
    )
    s3_client = session.client('s3')
    healthlake_client = session.client('healthlake')

    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"S3 bucket '{bucket_name}' exists and is accessible.")
    except ClientError as e:
        print(f"Could not access S3 bucket '{bucket_name}': {e}")
        sys.exit(1)

    try:
        healthlake_client.describe_fhir_datastore(DatastoreId=datastore_id)
        print(f"HealthLake Data Store ID '{datastore_id}' is valid and accessible.")
    except ClientError as e:
        print(f"Could not access HealthLake Data Store ID '{datastore_id}': {e}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  ./hiosctl.py store-s3 --bucket-name your-bucket-name --path /path/to/synthea_fhir_bundles")
        print("  ./hiosctl.py ingest-healthlake --bucket-name your-bucket-name --datastore-id your-datastore-id --darole-arn your-darole-arn")
        print("  ./hiosctl.py doctor --bucket-name your-bucket-name --datastore-id your-datastore-id")
        sys.exit(1)

    command = sys.argv[1]

    if command == "store-s3":
        if len(sys.argv) != 6 or sys.argv[2] != "--bucket-name" or sys.argv[4] != "--path":
            print("Usage: ./hiosctl.py store-s3 --bucket-name your-bucket-name --path /path/to/synthea_fhir_bundles")
            sys.exit(1)
        bucket_name = sys.argv[3]
        synthea_fhir_bundles = sys.argv[5]
        if not os.path.isdir(synthea_fhir_bundles):
            print(f"The specified path {synthea_fhir_bundles} is not a directory.")
            sys.exit(1)
        store_s3(bucket_name, synthea_fhir_bundles)
    elif command == "ingest-healthlake":
        if len(sys.argv) != 8 or sys.argv[2] != "--bucket-name" or sys.argv[4] != "--datastore-id" or sys.argv[6] != "--darole-arn":
            print("Usage: ./hiosctl.py ingest-healthlake --bucket-name your-bucket-name --datastore-id your-datastore-id --darole-arn your-darole-arn")
            sys.exit(1)
        bucket_name = sys.argv[3]
        datastore_id = sys.argv[5]
        data_access_role_arn = sys.argv[7]
        ingest_healthlake(bucket_name, datastore_id, data_access_role_arn)
    elif command == "doctor":
        if len(sys.argv) != 6 or sys.argv[2] != "--bucket-name" or sys.argv[4] != "--datastore-id":
            print("Usage: ./hiosctl.py doctor --bucket-name your-bucket-name --datastore-id your-datastore-id")
            sys.exit(1)
        bucket_name = sys.argv[3]
        datastore_id = sys.argv[5]
        doctor(bucket_name, datastore_id)
    else:
        print(f"Unknown command: {command}")
        print("Usage:")
        print("  ./hiosctl.py store-s3 --bucket-name your-bucket-name --path /path/to/synthea_fhir_bundles")
        print("  ./hiosctl.py ingest-healthlake --bucket-name your-bucket-name --datastore-id your-datastore-id --darole-arn your-darole-arn")
        print("  ./hiosctl.py doctor --bucket-name your-bucket-name --datastore-id your-datastore-id")
        sys.exit(1)
