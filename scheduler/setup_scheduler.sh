#!/bin/bash
# Setup Cloud Scheduler triggers for the Talent Lakehouse pipeline
# Run this script once to create all scheduled jobs
# Pipeline runs daily at 02:00-03:30 BRT (05:00-06:30 UTC)

PROJECT=talent-lakehouse-poc
REGION=southamerica-east1
SA=talent-lakehouse-sa@talent-lakehouse-poc.iam.gserviceaccount.com
BASE_URI="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT}/jobs"

gcloud scheduler jobs create http schedule-postgres-to-gcs \
  --location=$REGION --schedule="0 5 * * *" \
  --uri="${BASE_URI}/job-postgres-to-gcs:run" \
  --message-body="{}" --oauth-service-account-email=$SA \
  --project=$PROJECT

gcloud scheduler jobs create http schedule-api-to-gcs \
  --location=$REGION --schedule="30 5 * * *" \
  --uri="${BASE_URI}/job-api-to-gcs:run" \
  --message-body="{}" --oauth-service-account-email=$SA \
  --project=$PROJECT

gcloud scheduler jobs create http schedule-gcs-to-bq \
  --location=$REGION --schedule="0 6 * * *" \
  --uri="${BASE_URI}/job-gcs-to-bq:run" \
  --message-body="{}" --oauth-service-account-email=$SA \
  --project=$PROJECT

gcloud scheduler jobs create http schedule-dbt-run \
  --location=$REGION --schedule="20 6 * * *" \
  --uri="${BASE_URI}/job-dbt-run:run" \
  --message-body="{}" --oauth-service-account-email=$SA \
  --project=$PROJECT

gcloud scheduler jobs create http schedule-dbt-test \
  --location=$REGION --schedule="30 6 * * *" \
  --uri="${BASE_URI}/job-dbt-test:run" \
  --message-body="{}" --oauth-service-account-email=$SA \
  --project=$PROJECT
