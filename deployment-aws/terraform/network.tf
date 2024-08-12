resource "aws_vpc" "main" {
  cidr_block                       = "172.16.0.0/16"
  assign_generated_ipv6_cidr_block = true
  enable_dns_hostnames             = true
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }
  route {
    ipv6_cidr_block = "::/0"
    gateway_id      = aws_internet_gateway.main.id
  }
}


resource "aws_subnet" "public" {
  vpc_id                          = aws_vpc.main.id
  availability_zone               = "${data.aws_region.current.name}a"
  cidr_block                      = cidrsubnet(aws_vpc.main.cidr_block, 8, 0)
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.main.ipv6_cidr_block, 8, 0)
  map_public_ip_on_launch         = true
  assign_ipv6_address_on_creation = true
  tags                            = {
    Name = "${local.common_tags.Name}-public"
  }
}

resource "aws_subnet" "extra" {
  # currently unused but ALB requires at least two subnets in two different AZs
  vpc_id                          = aws_vpc.main.id
  availability_zone               = "${data.aws_region.current.name}b"
  cidr_block                      = cidrsubnet(aws_vpc.main.cidr_block, 8, 1)
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.main.ipv6_cidr_block, 8, 1)
  map_public_ip_on_launch         = true
  assign_ipv6_address_on_creation = true
  tags                            = {
    Name = "${local.common_tags.Name}-extra"
  }
}

resource "aws_route_table_association" "public" {
  route_table_id = aws_route_table.public.id
  subnet_id      = aws_subnet.public.id
}

resource "aws_route_table_association" "extra" {
  route_table_id = aws_route_table.public.id
  subnet_id      = aws_subnet.extra.id
}

resource "aws_subnet" "private_a" {
  vpc_id                          = aws_vpc.main.id
  availability_zone               = "${data.aws_region.current.name}a"
  cidr_block                      = cidrsubnet(aws_vpc.main.cidr_block, 8, 10)
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.main.ipv6_cidr_block, 8, 10)
  assign_ipv6_address_on_creation = true
  tags                            = {
    Name = "${local.common_tags.Name}-private-a"
  }
}
# currently unused but required for RDS aws_db_subnet_group
resource "aws_subnet" "private_b" {
  vpc_id                          = aws_vpc.main.id
  availability_zone               = "${data.aws_region.current.name}b"
  cidr_block                      = cidrsubnet(aws_vpc.main.cidr_block, 8, 11)
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.main.ipv6_cidr_block, 8, 11)
  assign_ipv6_address_on_creation = true
  tags                            = {
    Name = "${local.common_tags.Name}-private-b"
  }
}
resource "aws_db_subnet_group" "default" {
  name       = local.common_tags.Name
  subnet_ids = [aws_subnet.private_a.id, aws_subnet.private_b.id]
}

resource "aws_security_group" "database" {
  name        = "${local.common_tags.Name}-database"
  description = "Allow incoming connections to PostgreSQL instance from the API server"
  vpc_id      = aws_vpc.main.id
  ingress {
    description     = "PostrgeSQL"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.api_server.id]
  }
}

resource "aws_security_group" "load_balancer" {
  name        = "${local.common_tags.Name}-loadbalancer"
  description = "Allow HTTP and HTTPS access"
  vpc_id      = aws_vpc.main.id
}
# using standalone security group rules for ALB to avoid cycle errors
resource "aws_security_group_rule" "http_ingress" {
  description       = "Allow inbound HTTP from Internet to ALB"
  type              = "ingress"
  from_port         = 80
  to_port           = 80
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
  ipv6_cidr_blocks  = ["::/0"]
  security_group_id = aws_security_group.load_balancer.id
}
resource "aws_security_group_rule" "https_ingress" {
  description       = "Allow inbound HTTPS from Internet to ALB"
  type              = "ingress"
  from_port         = 443
  to_port           = 443
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
  ipv6_cidr_blocks  = ["::/0"]
  security_group_id = aws_security_group.load_balancer.id
}
resource "aws_security_group_rule" "http_egress" {
  description              = "Allow HTTP from ALB to web server"
  type                     = "egress"
  from_port                = 80
  to_port                  = 80
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.api_server.id
  security_group_id        = aws_security_group.load_balancer.id
}

resource "aws_security_group" "api_server" {
  name        = "${local.common_tags.Name}-apiserver"
  description = "Allow inbound HTTP from ALB and SSH from the Internet"
  vpc_id      = aws_vpc.main.id
  ingress {
    description     = "HTTP from ALB"
    from_port       = 80
    to_port         = 80
    protocol        = "tcp"
    security_groups = [aws_security_group.load_balancer.id]
  }
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

# All the VPC endpoints get this security group which will allow
# connections from ECS instances
resource "aws_security_group" "endpoint_security_group" {
  name        = "${local.common_tags.Name}-endpoint-sg"
  description = "For endpoint to allow ecs communications"
  vpc_id      = aws_vpc.main.id
  ingress {
    description      = "Allow ingress from ECS sg"
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    security_groups  = [aws_security_group.ecs_instance_security_group.id]
  }
}

# The security group associatedc with the ephemerial ECS instances.
resource "aws_security_group" "ecs_instance_security_group" {
  name        = "${local.common_tags.Name}-ecs-sg"
  description = "Allow everything out, nothing in"
  vpc_id      = aws_vpc.main.id
  egress {
    description      = "Allow all egress"
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }
}

# For ECS instances launched into the private subnet, they can communicate
# with the container registry via this vpc endpoint
resource "aws_vpc_endpoint" "ecr_dkr" {
  vpc_id            = aws_vpc.main.id
  service_name      = "com.amazonaws.${data.aws_region.current.name}.ecr.dkr"
  vpc_endpoint_type = "Interface"
  subnet_ids        =  [aws_subnet.private_a.id, aws_subnet.private_b.id]
  security_group_ids = [
    aws_security_group.endpoint_security_group.id
  ]
  policy =<<POLICY
  {
    "Statement": [
      {
        "Sid": "AllowPull",
        "Principal": {
          "AWS": "${aws_iam_role.ecs_execution_role.arn}"
        },
        "Action": [
          "ecr:BatchGetImage",
          "ecr:GetDownloadUrlForLayer",
          "ecr:GetAuthorizationToken"
        ],
        "Effect": "Allow",
        "Resource": "*"
      }
    ]
  }
  POLICY
  private_dns_enabled = true
}

# For ECS instances launched into the private subnet, they can communicate
# with the container registry via this vpc endpoint
resource "aws_vpc_endpoint" "ecr_api" {
  vpc_id            = aws_vpc.main.id
  service_name      = "com.amazonaws.${data.aws_region.current.name}.ecr.api"
  vpc_endpoint_type = "Interface"
  subnet_ids        =  [aws_subnet.private_a.id, aws_subnet.private_b.id]
  security_group_ids = [
    aws_security_group.endpoint_security_group.id
  ]
  policy =<<POLICY
  {
    "Statement": [
      {
        "Sid": "AllowPull",
        "Principal": {
          "AWS": "${aws_iam_role.ecs_execution_role.arn}"
        },
        "Action": [
          "ecr:BatchGetImage",
          "ecr:GetDownloadUrlForLayer",
          "ecr:GetAuthorizationToken"
        ],
        "Effect": "Allow",
        "Resource": "*"
      }
    ]
  }
  POLICY
  private_dns_enabled = true
}


# Permits the ECS instances in the private subnet to communicate with 
# logging mechanism
resource "aws_vpc_endpoint" "logs_endpoint" {
  vpc_id            = aws_vpc.main.id
  service_name      = "com.amazonaws.${data.aws_region.current.name}.logs"
  vpc_endpoint_type = "Interface"
  subnet_ids        =  [aws_subnet.private_a.id, aws_subnet.private_b.id]
  security_group_ids = [
    aws_security_group.endpoint_security_group.id
  ]
  private_dns_enabled = true
}

# Although we don't explicitly create a route table for the 
# private subnets, it is created automatically and associated
# with both
# data "aws_route_table" "private_route_table" {
#   vpc_id    = aws_vpc.main.id
#   subnet_id = aws_subnet.private_a.id
# }

resource "aws_route_table" "private_route_table" {
  vpc_id = aws_vpc.main.id 
}

resource "aws_route_table_association" "private_a" {
  route_table_id = aws_route_table.private_route_table.id
  subnet_id      = aws_subnet.private_a.id
}

resource "aws_route_table_association" "private_b" {
  route_table_id = aws_route_table.private_route_table.id
  subnet_id      = aws_subnet.private_b.id
}

# To pull container image layers, the ECS instances need
# access to S3 which we provide through a Gateway
resource "aws_vpc_endpoint" "s3_gateway" {
  vpc_id            = aws_vpc.main.id
  service_name      = "com.amazonaws.${data.aws_region.current.name}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids = [
    aws_route_table.private_route_table.id
  ]
}