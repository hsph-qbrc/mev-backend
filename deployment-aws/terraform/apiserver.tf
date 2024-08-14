# This allows the EC2 instance to assume a particular role.
# This enables us, for instance, to give the EC2 server 
# permissions to access the S3 buckets. Note that this block alone
# does not give that permission.
resource "aws_iam_role" "api_server_role" {
  name               = "${local.common_tags.Name}-api"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [
      {
        Action    = "sts:AssumeRole"
        Effect    = "Allow"
        Sid       = ""
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      },
    ]
  })
}

resource "aws_iam_role_policy" "server_s3_access" {
  name   = "AllowAccessToStorageBuckets"
  role   = aws_iam_role.api_server_role.id
  policy = jsonencode(
    {
      Version   = "2012-10-17",
      Statement = [
        {
          Effect   = "Allow",
          Action   = ["s3:GetObject", "s3:PutObject", "s3:PutObjectAcl", "s3:DeleteObject"],
          Resource = [
            "arn:aws:s3:::${aws_s3_bucket.api_storage_bucket.id}/*",
            "arn:aws:s3:::${aws_s3_bucket.nextflow_storage_bucket.id}/*",
            "arn:aws:s3:::${local.globus_bucket}/*"
          ]
        },
        {
          Effect   = "Allow",
          Action   = ["s3:GetObject"],
          Resource = [
            "arn:aws:s3:::${var.public_data_bucket_name}/*",
          ]
        },
        {
          Effect   = "Allow",
          Action   = ["s3:ListBucket"],
          Resource = [
            "arn:aws:s3:::${aws_s3_bucket.api_storage_bucket.id}",
            "arn:aws:s3:::${aws_s3_bucket.nextflow_storage_bucket.id}",
            "arn:aws:s3:::${local.globus_bucket}"
          ]
        }
      ]
    }
  )
}

resource "aws_iam_role_policy" "server_batch_access" {
  name   = "AllowAccessToBatch"
  role   = aws_iam_role.api_server_role.id
  policy = jsonencode(
    {
      Version   = "2012-10-17",
      Statement = [
        {
          Effect   = "Allow",
          Action   = [
            "batch:DescribeJobQueues",
            "batch:CancelJob",
            "batch:SubmitJob",
            "batch:ListJobs",
            "batch:DescribeComputeEnvironments",
            "batch:TerminateJob",
            "batch:DescribeJobs",
            "batch:RegisterJobDefinition",
            "batch:DescribeJobDefinitions",
            "batch:TagResource"
          ],
          Resource = ["*"]
        }
      ]
    }
  )
}

# For adding SSM to the instance:
resource "aws_iam_role_policy_attachment" "api_server_ssm" {
  role       = aws_iam_role.api_server_role.id
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy_attachment" "api_server_cloudwatch" {
  role       = aws_iam_role.api_server_role.id
  policy_arn = "arn:aws:iam::aws:policy/AWSOpsWorksCloudWatchLogs"
}

resource "aws_iam_role_policy_attachment" "api_server_cloudwatch_agent" {
  role       = aws_iam_role.api_server_role.id
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy"
}

resource "aws_iam_role_policy" "log_retention" {
  name   = "AllowCLoudWatchLogRetention"
  role   = aws_iam_role.api_server_role.id
  policy = jsonencode(
    {
      Version   = "2012-10-17",
      Statement = [
        {
          Effect   = "Allow",
          Action   = ["logs:PutRetentionPolicy"],
          Resource = "*"
        }
      ]
    }
  )
}

resource "aws_iam_instance_profile" "api_server_instance_profile" {
  name = "${local.common_tags.Name}-api"
  role = aws_iam_role.api_server_role.name
}

resource "aws_ebs_volume" "data_volume" {
  availability_zone = "${data.aws_region.current.name}a"
  size              = 300
  type              = "gp3"
  final_snapshot    = true
  snapshot_id       = var.data_volume_snapshot_id
  encrypted         = true
  tags = {
    Name = "${local.common_tags.Name}-ebs"
  }
}

resource "aws_volume_attachment" "data_ebs_attachment" {
  # a valid device name-- note that this is NOT the device name
  # that you will see on aws_instance.api. That is typically
  # something like /dev/nvme1n1. We still need to supply a device
  # name here, however.
  # https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/device_naming.html
  device_name           = "/dev/sdh"
  volume_id             = aws_ebs_volume.data_volume.id
  instance_id           = aws_instance.api.id
}

resource "aws_instance" "api" {
  # Ubuntu 22.04 LTS x86-64 LTS https://cloud-images.ubuntu.com/locator/ec2/
  ami                    = "ami-00d5c4dd05b5467c4"
  instance_type          = "t3.xlarge"
  monitoring             = true
  subnet_id              = aws_subnet.public.id
  vpc_security_group_ids = [aws_security_group.api_server.id]
  ebs_optimized          = true
  iam_instance_profile   = aws_iam_instance_profile.api_server_instance_profile.name
  tags                   = {
    Name = "${local.common_tags.Name}-api"
  }
  volume_tags = merge(local.common_tags, { Name = "${local.common_tags.Name}-api" })
  root_block_device {
    volume_type = "gp3"
    volume_size = 12
    encrypted   = true
  }
  user_data_replace_on_change = true
  user_data                   = <<-EOT
  #!/usr/bin/bash -ex

  # https://serverfault.com/a/670688
  export DEBIAN_FRONTEND=noninteractive

  # to help Puppet determine the correct node name
  /usr/bin/hostnamectl set-hostname ${local.backend_cname}

  # install Puppet
  OS_CODENAME=$(/usr/bin/lsb_release -sc)
  PUPPET_PACKAGE=puppet8-release-$${OS_CODENAME}.deb
  /usr/bin/curl -sO "https://apt.puppetlabs.com/$${PUPPET_PACKAGE}"
  /usr/bin/dpkg -i "$PUPPET_PACKAGE"
  /usr/bin/apt-get -qq update
  /usr/bin/apt-get -qq -y install puppet-agent nvme-cli

  # configure WebMEV
  export PROJECT_ROOT=/srv/mev-backend
  /usr/bin/mkdir $PROJECT_ROOT
  /usr/bin/chown ubuntu:ubuntu $PROJECT_ROOT
  /usr/bin/su -c "git clone https://github.com/web-mev/mev-backend.git $PROJECT_ROOT" ubuntu
  /usr/bin/su -c "cd $PROJECT_ROOT && /usr/bin/git checkout -q ${local.commit_id}" ubuntu

  # install and configure librarian-puppet
  export PUPPET_ROOT="$PROJECT_ROOT/deployment-aws/puppet"
  /opt/puppetlabs/puppet/bin/gem install librarian-puppet -v 5.0.0 --no-document
  # need to set $HOME: https://github.com/rodjek/librarian-puppet/issues/258
  export HOME=/root
  /opt/puppetlabs/puppet/bin/librarian-puppet config path /opt/puppetlabs/puppet/modules --global
  /opt/puppetlabs/puppet/bin/librarian-puppet config tmp /tmp --global
  cd $PUPPET_ROOT
  PATH=$PATH:/opt/puppetlabs/bin
  /opt/puppetlabs/puppet/bin/librarian-puppet install

  # get the volume ID for the attached EBS volume (non-root)
  EBS_VOLUME_ID='${aws_ebs_volume.data_volume.id}'
  DEVICE_ID=$(python3 $PROJECT_ROOT/etc/get_device_id.py -i $EBS_VOLUME_ID)

  # configure and run Puppet
  export FACTER_ADMIN_EMAIL_CSV='${var.admin_email_csv}'
  export FACTER_AWS_BATCH_QUEUE='${aws_batch_job_queue.nextflow.name}'
  export FACTER_AWS_ECS_CLUSTER='${aws_ecs_cluster.ecs.name}'
  export FACTER_AWS_ECS_SECURITY_GROUP='${aws_security_group.ecs_instance_security_group.name}'
  export FACTER_AWS_ECS_SUBNET='${aws_subnet.public.id}'
  export FACTER_AWS_REGION='${data.aws_region.current.name}'
  export FACTER_BACKEND_DOMAIN='${var.backend_domain}'
  export FACTER_CLOUDWATCH_LOG_GROUP='${aws_cloudwatch_log_group.default.name}'
  export FACTER_CONTAINER_REGISTRY='${var.container_registry}'
  export FACTER_DATABASE_HOST='${aws_db_instance.default.address}'
  export FACTER_DATABASE_SUPERUSER='${aws_db_instance.default.username}'
  export FACTER_DATABASE_SUPERUSER_PASSWORD='${var.database_superuser_password}'
  export FACTER_DATABASE_USER_PASSWORD='${var.database_password}'
  export FACTER_DATA_VOLUME_DEVICE_NAME=$DEVICE_ID
  export FACTER_DEPLOYMENT_STACK='${local.stack}'
  export FACTER_DJANGO_CORS_ORIGINS='https://${var.frontend_domain},${var.additional_cors_origins}'
  export FACTER_DJANGO_SETTINGS_MODULE='${var.django_settings_module}'
  export FACTER_DJANGO_SUPERUSER_EMAIL='${var.django_superuser_email}'
  export FACTER_DJANGO_SUPERUSER_PASSWORD='${var.django_superuser_password}'
  export FACTER_ENABLE_REMOTE_JOB_RUNNERS='${var.enable_remote_job_runners}'
  export FACTER_EMAIL_HOST_USER="${aws_iam_access_key.ses_user.id}"
  export FACTER_EMAIL_HOST_PASSWORD="${aws_iam_access_key.ses_user.ses_smtp_password_v4}"
  export FACTER_FROM_EMAIL='${var.from_email}'
  export FACTER_FRONTEND_DOMAIN='${var.frontend_domain}'
  export FACTER_GLOBUS_APP_CLIENT_ID='${var.globus == null ? "" : var.globus.app_client_uuid}'
  export FACTER_GLOBUS_APP_CLIENT_SECRET='${var.globus == null ? "" : var.globus.app_client_secret}'
  export FACTER_GLOBUS_BUCKET_NAME='${local.globus_bucket}'
  export FACTER_GLOBUS_ENDPOINT_CLIENT_SECRET='${var.globus == null ? "" : var.globus.endpoint_client_secret}'
  export FACTER_GLOBUS_ENDPOINT_CLIENT_UUID='${var.globus == null ? "" : var.globus.endpoint_client_uuid}'
  export FACTER_GLOBUS_ENDPOINT_ID='${var.globus == null ? "" : var.globus.endpoint_id}'
  export FACTER_GOOGLE_OAUTH2_CLIENT_ID='${var.google_oauth2_client_id}'
  export FACTER_GOOGLE_OAUTH2_CLIENT_SECRET='${var.google_oauth2_client_secret}'
  export FACTER_NEXTFLOW_BUCKET_NAME='${aws_s3_bucket.nextflow_storage_bucket.id}'
  export FACTER_PUBLIC_DATA_BUCKET_NAME='${var.public_data_bucket_name}'
  export FACTER_SENTRY_URL='${var.sentry_url}'
  export FACTER_STORAGE_LOCATION='${var.storage_location}'
  export FACTER_STORAGE_BUCKET_NAME='${aws_s3_bucket.api_storage_bucket.id}'

  /opt/puppetlabs/bin/puppet apply $PUPPET_ROOT/manifests/site.pp
  EOT
}
