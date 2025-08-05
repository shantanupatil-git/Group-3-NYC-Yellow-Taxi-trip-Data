# main.tf
provider "aws" {
  region = "us-east-1"
}

# Create S3 buckets (if not already created)
resource "aws_s3_bucket" "source_bucket" {
  bucket = "opbkt-glue"
  force_destroy = true
}

resource "aws_s3_bucket" "target_bucket" {
  bucket = "gpbkt"
  force_destroy = true
}

# Upload the ETL script to source bucket
resource "aws_s3_object" "etl_script" {
  bucket = aws_s3_bucket.source_bucket.bucket
  key    = "scripts/etl-glue-script.py"
  source = "../etl-glue-script.py"  # Must exist locally before running `terraform apply`
  etag   = filemd5("../etl-glue-script.py")
}

# Glue database
resource "aws_glue_catalog_database" "nyc_db" {
  name = "nyc-yellow-taxi-trip-data"
}

# Glue job
resource "aws_glue_job" "firstjob" {
  name     = "firstjob"
  role_arn = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/labrole"

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.source_bucket.bucket}/${aws_s3_object.etl_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"         = "python"
    "--SOURCE_PATH"          = "s3://${aws_s3_bucket.source_bucket.bucket}/processed-output/"
    "--TARGET_PATH"          = "s3://${aws_s3_bucket.target_bucket.bucket}/cleaned/"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-glue-datacatalog"         = "true"
  }

  glue_version     = "4.0"
  number_of_workers = 2
  worker_type      = "G.1X"
}

# Glue crawler
resource "aws_glue_crawler" "nyc_crawler" {
  name         = "nyc-crawler"
  role         = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/labrole"
  database_name = aws_glue_catalog_database.nyc_db.name

  s3_target {
    path = "s3://${aws_s3_bucket.target_bucket.bucket}/cleaned/"
  }

  depends_on = [aws_glue_job.firstjob]
}

data "aws_caller_identity" "current" {}
