output "source_bucket" {
  value = var.raw_bucket_name
}

output "target_bucket" {
  value = var.cleaned_bucket_name
}

output "glue_database_name" {
  value = aws_glue_catalog_database.this.name
}

output "glue_job_name" {
  value = aws_glue_job.this.name
}

output "crawler_name" {
  value = aws_glue_crawler.this.name
}
