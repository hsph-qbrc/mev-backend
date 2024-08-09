# This allows the ECS instance to assume a particular role.
# This enables us, for instance, to give 
# permissions to access the S3 buckets. Note that this block alone
# does not give that permission. The task role is what is given
# to the ECS instances spun up.
resource "aws_iam_role" "ecs_task_role" {
  name = "${local.common_tags.Name}-ecs-task-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action    = "sts:AssumeRole"
        Effect    = "Allow"
        Sid       = ""
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "ecs_s3_access" {
  name   = "AllowECSAccessToStorageBucket"
  role   = aws_iam_role.ecs_task_role.id

  policy = jsonencode(
    {
      "Version": "2012-10-17",
      "Statement": [
        {
          "Effect": "Allow",
          "Action": ["s3:GetObject", "s3:PutObject", "s3:PutObjectAcl"],
          "Resource": ["arn:aws:s3:::${aws_s3_bucket.api_storage_bucket.id}", 
                       "arn:aws:s3:::${aws_s3_bucket.api_storage_bucket.id}/*"
          ]
        }
      ]
    }
  )
}


resource "aws_iam_role_policy" "efs_ap" {
    name   = "EFSAccessPointForECS"
    role   = "${aws_iam_role.ecs_task_role.id}"
  
    policy = jsonencode(
      {
        "Version": "2012-10-17",
        "Statement": [
          {
            "Effect": "Allow",
            "Action": ["elasticfilesystem:ClientMount", 
                       "elasticfilesystem:ClientWrite"
            ],
            "Resource": aws_efs_file_system.efs.arn
          }
        ]
      }
    )
  }



# This role is given to the ECS service to handle the creation of
# ECS tasks, etc.
resource "aws_iam_role" "ecs_execution_role" {
  name = "${local.common_tags.Name}-ecs-execution-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action    = "sts:AssumeRole"
        Effect    = "Allow"
        Sid       = ""
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        }
      },
    ]
  })
}


resource "aws_iam_role_policy_attachment" "ecs_execution_role" {
  role       = aws_iam_role.ecs_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}


resource "aws_cloudwatch_log_group" "ecs" {
  name = "${local.common_tags.Name}-ecs-logs"
}


resource "aws_ecs_cluster" "ecs" {
  name = "${local.common_tags.Name}-ecs-cluster"

  configuration {
    execute_command_configuration {
      logging = "OVERRIDE"
      log_configuration {
        cloud_watch_encryption_enabled = true
        cloud_watch_log_group_name     = aws_cloudwatch_log_group.ecs.name
      }
    }
  }
}

