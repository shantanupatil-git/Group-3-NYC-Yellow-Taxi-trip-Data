resource "aws_s3_object" "etl_script" {
  bucket = aws_s3_bucket.source_bucket.id
  key    = var.script_s3_key
  source = "../etl-glue-script.py"
  etag   = filemd5("../etl-glue-script.py")
}
