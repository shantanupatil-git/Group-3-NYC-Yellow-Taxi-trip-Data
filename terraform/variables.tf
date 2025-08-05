variable "region" {
  default = "us-east-1"
}

variable "raw_bucket_name" {
  default = "opbkt-glue"
}

variable "cleaned_bucket_name" {
  default = "gpbkt"
}

variable "glue_db_name" {
  default = "nyc-yellow-taxi-trip-data"
}

variable "glue_job_name" {
  default = "firstjob"
}

variable "glue_crawler_name" {
  default = "nyc-crawler"
}

variable "etl_script_s3_key" {
  default = "scripts/etl-glue-script.py"
}
