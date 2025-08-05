output "source_bucket" {
  value = aws_s3_bucket.source_bucket.bucket
}

output "target_bucket" {
  value = aws_s3_bucket.target_bucket.bucket
}

output "glue_database_name" {
  value = aws_glue_catalog_database.nyc_db.name
}

output "glue_job_name" {
  value = aws_glue_job.etl_job.name
}

output "crawler_name" {
  value = aws_glue_crawler.nyc_crawler.name
}
