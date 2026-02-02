#!/bin/bash
set -e

# 1) Reset (ignore errors if not found)
airflow connections delete aws_credentials || true
airflow connections delete redshift || true

# 2) Add AWS connection (no URI encoding headaches)
airflow connections add aws_credentials \
  --conn-type aws \
  --conn-login "AWS_ID" \
  --conn-password "AWS_KEY"

# 3) Add Redshift connection
# NOTE: For most Sparkify projects, conn-type should be postgres (Redshift uses Postgres protocol)
airflow connections add redshift \
  --conn-type postgres \
  --conn-host "mila-udacity.947755364128.us-east-1.redshift-serverless.amazonaws.com" \
  --conn-schema "dev" \
  --conn-login "awsuser" \
  --conn-password "milaR3dsh1ft" \
  --conn-port 5439

# 4) Variables
airflow variables set s3_bucket "mila-vas-project"
airflow variables set s3_prefix "data-pipelines"

# 5) Verify (optional)
airflow connections get aws_credentials
airflow connections get redshift
airflow variables list