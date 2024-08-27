resource "aws_s3_bucket" "api_storage_bucket" {
    bucket = "${local.stack}-webmev-storage"
}

resource "aws_s3_bucket" "job_storage_bucket" {
    bucket = "${local.stack}-webmev-job-storage"
}

resource "aws_s3_bucket" "logging" {
    bucket = "${local.stack}-webmev-backend-logs"
}

resource "aws_s3_bucket_cors_configuration" "storage_bucket_cors" {
  bucket = aws_s3_bucket.api_storage_bucket.id

  cors_rule {
    allowed_headers = ["RANGE", "Cache-control", "If-None-Match", "Content-Type"]
    allowed_methods = ["GET", "HEAD"]
    allowed_origins = ["https://${var.frontend_domain}"]
    expose_headers  = ["Content-Length", "Content-Range", "Content-Type"]
    max_age_seconds = 3000
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "main_storage" {
  bucket = aws_s3_bucket.api_storage_bucket.bucket

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "job_storage" {
  bucket = aws_s3_bucket.job_storage_bucket.bucket

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_policy" "logging_bucket"  {
  bucket = aws_s3_bucket.logging.id
  policy = data.aws_iam_policy_document.logging_bucket_policy_doc.json
}

# For specifying the policies of the log bucket, we use an `aws_iam_policy_document`
# resource rather than using HEREDOC syntax or otherwise. See:
# https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/s3_bucket_policy
data "aws_iam_policy_document" "logging_bucket_policy_doc" {
  
  statement {
    sid = "AllowLBLogging"

    # Note the "033677994240" corresponds to the LB account
    # ID for us-east-2. Change this if you change the region.
    principals {
      type = "AWS"
      identifiers = ["arn:aws:iam::033677994240:root"]
    }

    effect = "Allow"

    actions = ["s3:PutObject"]

    resources = [
      "arn:aws:s3:::${aws_s3_bucket.logging.id}/AWSLogs/${data.aws_caller_identity.current.account_id}/*"
    ]
    
  }

  statement {

    sid = "AWSCloudTrailAclCheck"

    principals {
      type = "Service"
      identifiers = ["cloudtrail.amazonaws.com"]
    }

    effect = "Allow"

    actions = [
      "s3:GetBucketAcl"
    ]

    resources = [
      "arn:aws:s3:::${aws_s3_bucket.logging.id}"
    ]

    condition {
      test = "StringEquals"
      variable = "aws:SourceArn"
      values = ["arn:aws:cloudtrail:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:trail/${local.common_tags.Name}"]
    }

  }

  statement {
    
    sid = "AWSCloudTrailWrite"

    principals {
      type = "Service"
      identifiers = ["cloudtrail.amazonaws.com"]
    }

    effect = "Allow"

    actions = [
      "s3:PutObject"
    ]

    resources = [
      "arn:aws:s3:::${aws_s3_bucket.logging.id}/AWSLogs/${data.aws_caller_identity.current.account_id}/*"
    ]

    condition  {
      test = "StringEquals"
      variable = "aws:SourceArn"
      values = ["arn:aws:cloudtrail:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:trail/${local.common_tags.Name}"]
    }

    condition  {
      test = "StringEquals"
      variable = "s3:x-amz-acl"
      values = ["bucket-owner-full-control"]
    }

  }
}
