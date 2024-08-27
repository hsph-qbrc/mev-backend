resource "aws_cloudtrail" "default" {
  name                          = local.common_tags.Name
  s3_bucket_name                = aws_s3_bucket.logging.id
  include_global_service_events = true

  event_selector {

    read_write_type           = "All"
    include_management_events = true

    data_resource {
      type = "AWS::S3::Object"
      values = [
        "${aws_s3_bucket.api_storage_bucket.arn}/",
        "${aws_s3_bucket.job_storage_bucket.arn}/",
      ]
    }
  }
}
