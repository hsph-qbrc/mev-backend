resource "aws_iam_role" "lambda" {
  name               = "${local.common_tags.Name}-lambda"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [
      {
        Action    = "sts:AssumeRole"
        Effect    = "Allow"
        Sid       = ""
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      },
    ]
  })
}

# lambda functions need access to the storage bucket 
# to view files.
resource "aws_iam_role_policy" "lambda_s3_access" {
  name   = "AllowLambdaAccessToStorageBuckets"
  role   = aws_iam_role.lambda.id
  policy = jsonencode(
    {
      Version   = "2012-10-17",
      Statement = [
        {
          Effect   = "Allow",
          Action   = ["s3:GetObject"],
          Resource = [
            "arn:aws:s3:::${aws_s3_bucket.api_storage_bucket.id}/*",
          ]
        }
      ]
    }
  )
}

resource "aws_iam_role_policy_attachment" "lambda" {
  role       = aws_iam_role.lambda.id
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_lambda_function" "resource_validation" {
  function_name = "${local.common_tags.Name}-resource-validation"
  timeout       = 10 # seconds
  image_uri     = var.ecr_image
  package_type  = "Image"
  role          = aws_iam_role.lambda.arn
}