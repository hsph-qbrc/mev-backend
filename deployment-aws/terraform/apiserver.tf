resource "random_password" "django_superuser" {
  length  = 8
  special = false
}

resource "aws_instance" "api" {
  # Ubuntu 20.04 LTS https://cloud-images.ubuntu.com/locator/ec2/
  ami                    = "ami-07f84a50d2dec2fa4"
  instance_type          = "t3.large"
  monitoring             = true
  subnet_id              = aws_subnet.public.id
  vpc_security_group_ids = [aws_security_group.api_server.id]
  ebs_optimized          = true
  key_name               = var.ssh_key_pair_name
  volume_tags = local.common_tags
  root_block_device {
    volume_type = "gp3"
  }
  user_data_replace_on_change = true
  user_data = <<-EOT
  #!/usr/bin/bash -ex

  # https://serverfault.com/a/670688
  export DEBIAN_FRONTEND=noninteractive

  # to help Puppet determine the correct node name
  /usr/bin/hostnamectl set-hostname ${var.backend_domain}

  # install Puppet
  CODENAME=$(/usr/bin/lsb_release -sc)
  /usr/bin/curl -sO "https://apt.puppetlabs.com/puppet7-release-$CODENAME.deb"
  /usr/bin/dpkg -i "puppet7-release-$CODENAME.deb"
  /usr/bin/apt-get -qq update
  /usr/bin/apt-get -qq -y install puppet-agent

  # configure WebMEV
  export PROJECT_ROOT=/srv/mev-backend
  /usr/bin/mkdir $PROJECT_ROOT
  /usr/bin/chown ubuntu:ubuntu $PROJECT_ROOT
  /usr/bin/su -c "git clone https://github.com/web-mev/mev-backend.git $PROJECT_ROOT" ubuntu
  /usr/bin/su -c "cd $PROJECT_ROOT && /usr/bin/git checkout -q ${var.git_commit}" ubuntu

  # install and configure librarian-puppet
  export PUPPET_ROOT="$PROJECT_ROOT/deployment-aws/puppet"
  /opt/puppetlabs/puppet/bin/gem install librarian-puppet -v 3.0.1 --no-document
  # need to set $HOME: https://github.com/rodjek/librarian-puppet/issues/258
  export HOME=/root
  /opt/puppetlabs/puppet/bin/librarian-puppet config path /opt/puppetlabs/puppet/modules --global
  /opt/puppetlabs/puppet/bin/librarian-puppet config tmp /tmp --global
  cd $PUPPET_ROOT
  PATH=$PATH:/opt/puppetlabs/bin
  /opt/puppetlabs/puppet/bin/librarian-puppet install

  # configure and run Puppet
  export FACTER_ADMIN_EMAIL_CSV='${var.admin_email_csv}'
  export FACTER_BACKEND_DOMAIN='${var.backend_domain}'
  export FACTER_CONTAINER_REGISTRY='${var.container_registry}'
  export FACTER_DATABASE_HOST='${aws_db_instance.default.address}'
  export FACTER_DATABASE_SUPERUSER='${aws_db_instance.default.username}'
  export FACTER_DATABASE_SUPERUSER_PASSWORD='${random_password.database_superuser.result}'
  export FACTER_DATABASE_USER_PASSWORD='${random_password.database_user.result}'
  export FACTER_DJANGO_SETTINGS_MODULE='${var.django_settings_module}'
  export FACTER_DJANGO_SUPERUSER_PASSWORD='${random_password.django_superuser.result}'
  export FACTER_ENABLE_REMOTE_JOB_RUNNERS='${var.enable_remote_job_runners}'
  export FACTER_FROM_EMAIL='${var.from_email}'
  export FACTER_FRONTEND_DOMAIN='${var.frontend_domain}'
  export FACTER_SENTRY_URL='${var.sentry_url}'
  export FACTER_STORAGE_LOCATION='${var.storage_location}'

  /opt/puppetlabs/bin/puppet apply $PUPPET_ROOT/manifests/site.pp
  EOT
}
