## How to prepare synthetic test fixtures (FHIR bundles JSON)

```bash
# download and unzip sample Synthea FHIR JSON files
$ cd support/synthetic-test-fixtures
$ wget https://synthetichealth.github.io/synthea-sample-data/downloads/latest/synthea_sample_data_fhir_latest.zip
$ mkdir fhir-bundles && cd fhir-bundles && unzip ../synthea_sample_data_fhir_latest.zip && cd ..
```

The directory should look like this now:

```
.
├── README.md
└── support
    └── synthetic-test-fixtures
        ├── fhir-bundles
        |   ├── Abraham100_Oberbrunner298_9dbb826d-0be6-e8f9-3254-dbac25d83be6.json
        |   ├── ...(many more files)
        |   └── Yon80_Kiehn525_54fe5c50-37cc-930b-8e3a-2c4e91bb6eec.json
        ├── README.md
        └── synthea_sample_data_fhir_latest.zip
```

## Learn how to use AWS HealthLake for a single file

```bash
curl -X POST "$AWS_HEALTHLAKE_API_ENDPOINT" \
     -H "Authorization: Bearer $AWS_HEALTHLAKE_API_KEY" \
     -H "Content-Type: application/json" \
     --data-binary "@ingest/Abraham100_Oberbrunner298_9dbb826d-0be6-e8f9-3254-dbac25d83be6.json"
```
