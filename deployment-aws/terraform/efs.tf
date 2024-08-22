# We use EFS to share files between atomic steps of ECS tasks.
# For instance, we have an AWS cli container which pulls images
# from S3 buckets on this EFS. Then, subsequent steps can access
# that file via EFS and won't need AWS cli installed.
resource "aws_efs_file_system" "efs" {
  creation_token  = "${local.common_tags.Name}-efs"
  throughput_mode = "elastic"
}

resource "aws_security_group" "efs_security_group" {
  name        = "${local.common_tags.Name}-efs-sg"
  description = "Allows ECS to communicate with EFS"
  vpc_id      = aws_vpc.main.id
  ingress {
    description      = "Allow limited ingress from ECS SG"
    from_port        = 2049
    to_port          = 2049
    protocol         = "tcp"
    security_groups  = [aws_security_group.ecs_instance_security_group.id]
  }
  ingress {
    description      = "Allow limited ingress from api server"
    from_port        = 2049
    to_port          = 2049
    protocol         = "tcp"
    security_groups  = [aws_security_group.api_server.id]
  }
}


resource "aws_efs_access_point" "efs_ap" {
  file_system_id = aws_efs_file_system.efs.id
  root_directory {
    path = "/share"
    creation_info {
      # these UID/GID are set by the mambaorg/micromamba
      # Docker image. Otherwise, we can't write to
      # the volume
      owner_gid   = 57439
      owner_uid   = 57439
      permissions = 0755
    }
  }
}

# mount point in public subnet "public"
resource "aws_efs_mount_target" "efs_mp_a" {
  file_system_id  = aws_efs_file_system.efs.id
  subnet_id       = aws_subnet.public.id
  security_groups = [aws_security_group.efs_security_group.id]
}

# mount point in public subnet "extra"
resource "aws_efs_mount_target" "efs_mp_b" {
  file_system_id  = aws_efs_file_system.efs.id
  subnet_id       = aws_subnet.extra.id
  security_groups = [aws_security_group.efs_security_group.id]
}