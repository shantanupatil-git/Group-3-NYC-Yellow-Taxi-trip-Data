provider "aws" {
  region = var.region
}

locals {
  glue_role_arn = "arn:aws:iam::298417083584:role/LabRole"
}

resource "aws_s3_bucket" "raw_bucket" {
  bucket = var.raw_bucket_name
}

resource "aws_s3_bucket" "cleaned_bucket" {
  bucket = var.cleaned_bucket_name
}

resource "aws_glue_catalog_database" "this" {
  name = var.glue_db_name
}

resource "aws_glue_job" "this" {
  name     = var.glue_job_name
  role_arn = local.glue_role_arn

  command {
    name            = "glueetl"
    script_location = "s3://${var.raw_bucket_name}/${var.etl_script_s3_key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language" = "python"
    "--SOURCE_PATH"  = "s3://${var.raw_bucket_name}/processed-output/"
    "--TARGET_PATH"  = "s3://${var.cleaned_bucket_name}/cleaned/"
  }

  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"
}

resource "aws_glue_crawler" "this" {
  name          = var.glue_crawler_name
  role          = local.glue_role_arn
  database_name = aws_glue_catalog_database.this.name

  s3_target {
    path = "s3://${var.cleaned_bucket_name}/cleaned/"
  }

  depends_on = [aws_glue_job.this]
}

data "aws_caller_identity" "current" {}
