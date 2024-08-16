resource "aws_iam_role" "batch_instance" {
  name               = "${local.common_tags.Name}-batch-instance"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17",
    Statement = [
      {
        Action    = "sts:AssumeRole",
        Effect    = "Allow",
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "batch_instance_container_service" {
  role       = aws_iam_role.batch_instance.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role"
}

resource "aws_iam_role_policy_attachment" "batch_instance_ssm" {
  role       = aws_iam_role.batch_instance.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy_attachment" "batch_instance_cloudwatch" {
  role       = aws_iam_role.batch_instance.name
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy"
}

resource "aws_iam_role_policy" "batch_instance_s3_access" {
  name   = "BatchInstanceS3Access"
  role   = aws_iam_role.batch_instance.id
  policy = jsonencode(
    {
      Version   = "2012-10-17",
      Statement = [
        {
          Effect   = "Allow",
          Resource = [
            "arn:aws:s3:::${aws_s3_bucket.job_storage_bucket.id}",
            "arn:aws:s3:::${aws_s3_bucket.job_storage_bucket.id}/*"
          ],
          Action = "s3:*"
        },
        {
          Effect   = "Allow",
          Resource = [
            "arn:aws:s3:::${aws_s3_bucket.api_storage_bucket.id}",
            "arn:aws:s3:::${aws_s3_bucket.api_storage_bucket.id}/*"
          ],
          Action = [
            "s3:Get*",
            "s3:List*"
          ]
        },
        {
          Effect   = "Deny",
          Resource = "arn:aws:s3:::${aws_s3_bucket.job_storage_bucket.id}",
          Action   = [
            "s3:DeleteBucket*",
            "s3:CreateBucket",
          ]
        }
      ]
    }
  )
}

resource "aws_iam_role_policy" "batch_instance_ebs" {
  name   = "AutoscaleEBS"
  role   = aws_iam_role.batch_instance.id
  policy = jsonencode({
    Version   = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "ec2:createTags",
          "ec2:createVolume",
          "ec2:attachVolume",
          "ec2:deleteVolume",
          "ec2:modifyInstanceAttribute",
          "ec2:describeVolumes",
        ],
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_instance_profile" "batch_instance" {
  name = "${local.common_tags.Name}-batch-instance"
  role = aws_iam_role.batch_instance.name
}

resource "aws_iam_role" "batch_service" {
  name               = "${local.common_tags.Name}-batch-service"
  assume_role_policy = jsonencode(
    {
      Version   = "2012-10-17",
      Statement = [
        {
          Action    = "sts:AssumeRole",
          Effect    = "Allow",
          Principal = {
            Service = "batch.amazonaws.com"
          }
        }
      ]
    }
  )
}

resource "aws_iam_role_policy_attachment" "batch_service" {
  role       = aws_iam_role.batch_service.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSBatchServiceRole"
}

resource "aws_iam_role_policy_attachment" "batch_ssm" {
  role       = aws_iam_role.batch_service.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_security_group" "batch_service" {
  name   = "${local.common_tags.Name}-batch"
  vpc_id = aws_vpc.main.id
  # implicit with AWS but Terraform requires this to be explicit
  egress {
    description      = "Allow all egress"
    from_port        = 0
    to_port          = 0
    protocol         = "all"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }
}

data "cloudinit_config" "batch_instance" {
  gzip = false
  part {
    content_type = "text/cloud-config"
    content      = <<-EOT
    #cloud-config
    repo_update: true
    repo_upgrade: security

    packages:
    - unzip

    runcmd:
    # install aws-cli v2 and copy the static binary in an easy to find location for bind-mounts into containers
    - curl -s -o /tmp/awscliv2.zip https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip
    - unzip -q /tmp/awscliv2.zip -d /run
    # the alternative install location is chosen so that we do not accidentally shadow
    # tools that might exist in canonical locations in the Docker container that executes
    # the workflow. For nextflow to move files, it mounts the directory containing
    # the AWS cli in the container; if that were /usr/local/bin/aws, then the volume
    # mount can shadow/block executables in the /usr/local/bin directory of the 
    # Docker image.
    - /run/aws/install --install-dir /opt/aws-cli --bin-dir /opt/aws-cli/bin
    EOT
  }
}

resource "aws_launch_template" "batch_instance" {
  image_id      = var.batch_instance_ami
  ebs_optimized = true
  # based on https://github.com/aws-samples/aws-genomics-workflows/blob/master/src/templates/gwfcore/gwfcore-launch-template.template.yaml
  user_data     = data.cloudinit_config.batch_instance.rendered
  block_device_mappings {
    device_name = "/dev/xvda"
    ebs {
      volume_size = 200
      volume_type = "gp3"
    }
  }
  monitoring {
    enabled = true
  }
}

resource "aws_batch_compute_environment" "nextflow" {
  type                            = "MANAGED"
  service_role                    = aws_iam_role.batch_service.arn
  depends_on                      = [aws_iam_role_policy_attachment.batch_service]
  # need to use name prefix and lifecycle meta-argument to avoid a bug
  # https://github.com/hashicorp/terraform-provider-aws/issues/13221
  # https://discuss.hashicorp.com/t/error-error-deleting-batch-compute-environment-cannot-delete-found-existing-jobqueue-relationship/5408/4
  compute_environment_name_prefix = "${local.common_tags.Name}-"
  lifecycle {
    create_before_destroy = true
  }
  compute_resources {
    instance_role      = aws_iam_instance_profile.batch_instance.arn
    instance_type      = ["c5","m5","r5"]
    max_vcpus          = 512
    min_vcpus          = 0
    security_group_ids = [aws_security_group.batch_service.id]
    subnets            = [aws_subnet.public.id, aws_subnet.extra.id]
    type               = "EC2"
    tags               = local.common_tags
    launch_template {
      launch_template_id = aws_launch_template.batch_instance.id
    }
  }
}

resource "aws_batch_job_queue" "nextflow" {
  name                 = local.common_tags.Name
  compute_environments = [aws_batch_compute_environment.nextflow.arn]
  priority             = 1
  state                = "ENABLED"
}
