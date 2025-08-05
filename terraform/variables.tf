variable "source_bucket_name" {
  description = "S3 bucket where raw data and script will be stored"
  default     = "opbkt-glue"
}

variable "target_bucket_name" {
  description = "S3 bucket where cleaned data will be stored"
  default     = "gpbkt"
}

variable "glue_database_name" {
  description = "Glue database name"
  default     = "nyc-yellow-taxi-trip-data"
}

variable "glue_job_name" {
  description = "Name of the Glue ETL job"
  default     = "firstjob"
}

variable "glue_crawler_name" {
  description = "Name of the Glue crawler"
  default     = "nyc-crawler"
}

variable "script_s3_key" {
  description = "S3 object key (path) for the ETL script"
  default     = "scripts/etl-glue-script.py"
}
