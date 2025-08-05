resource "aws_s3_object" "etl_script" {
  bucket = var.raw_bucket_name
  key    = var.etl_script_s3_key
  source = "${path.module}/../etl-glue-script.py"
  etag   = filemd5("${path.module}/../etl-glue-script.py")
}
