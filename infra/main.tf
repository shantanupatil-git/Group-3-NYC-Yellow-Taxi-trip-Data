
resource "aws_s3_bucket" "etl_bucket" {
  bucket = var.bucket_name_prefix
}

resource "aws_glue_catalog_database" "etl_db" {
  name = "nyc-taxi-trip_db"
}

locals {
  glue_role_arn = "arn:aws:iam::963702399712:role/LabRole"
}

resource "aws_glue_job" "etl_job" {
  name     = var.glue_job_name
  role_arn = local.glue_role_arn

  command {
    name            = "glueetl"
    script_location = var.script_s3_path
    python_version  = "3"
  }

  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"
}

resource "aws_glue_crawler" "etl_crawler" {
  name          = var.glue_crawler_name
  role          = local.glue_role_arn
  database_name = aws_glue_catalog_database.etl_db.name

  s3_target {
    path = "s3://${var.bucket_name_prefix}/transformeddata/"
  }

  depends_on = [aws_glue_job.etl_job]
}