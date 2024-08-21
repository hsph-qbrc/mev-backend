# @summary Install and configure WebMEV API server
#
# Provisions WebMEV API server on Vagrant and AWS
#
# @example
#   class { 'mevapi':
#   }
class mevapi (
  String                  $admin_email_csv,
  String                  $aws_batch_queue,
  String                  $aws_ecs_cluster,
  String                  $aws_ecs_execution_role,
  String                  $aws_ecs_log_group,
  String                  $aws_ecs_security_group,
  String                  $aws_ecs_subnet,
  String                  $aws_ecs_task_role,
  String                  $aws_efs_access_point,
  String                  $aws_efs_id,
  String                  $aws_region,
  Optional[String]        $app_user,
  String                  $backend_domain,
  String                  $cloudwatch_log_group,
  Optional[String]        $container_registry = 'github',
  String                  $database_host,
  Optional[String]        $database_superuser,
  Optional[String]        $database_superuser_password,
  String                  $database_user_password,
  String                  $data_root = '/data',
  String                  $data_volume_device_name,
  String                  $deployment_stack,
  String                  $django_cors_origins,
  Optional[String]        $django_settings_module,
  String                  $django_superuser_email,
  String                  $django_superuser_password,
  Optional[String]        $email_host = '',
  Optional[String]        $email_host_user = '',
  Optional[String]        $email_host_password = '',
  Optional[String]        $enable_remote_job_runners,
  Optional[String]        $from_email,
  String                  $frontend_domain,
  Optional[String]        $globus_app_client_id= '',
  Optional[String]        $globus_app_client_secret= '',
  Optional[String]        $globus_bucket_name= '',
  Optional[String]        $globus_endpoint_client_secret= '',
  Optional[String]        $globus_endpoint_client_uuid= '',
  Optional[String]        $globus_endpoint_id= '',
  Optional[String]        $google_oauth2_client_id='',
  Optional[String]        $google_oauth2_client_secret='',
  String                  $job_bucket_name,
  Optional[String]        $project_root,
  Optional[String]        $public_data_bucket_name='',
  Optional[String]        $sentry_url = '',
  Enum['local', 'remote'] $storage_location,
  String                  $storage_bucket_name,
) {
  if $facts['virtual'] == 'kvm' {
    $platform = 'aws'
  } else {
    # VirtualBox
    $platform = $facts['virtual']
  }

  $app_group = $app_user
  $database_user = $app_user
  $local_storage_dirname = "user_resources"

  $log_dir = '/var/log/mev'
  file { $log_dir:
    ensure => directory,
    owner  => $app_user,
    group  => $app_group,
  }

  # create the directory where ephemeral data will live
  file { $data_root:
    ensure => directory,
    owner  => $app_user,
    group  => $app_group,
  }

  # other directories that live under that data dir
  $data_dirs = [
    "${data_root}/pending_user_uploads",
    "${data_root}/tmp",
    "${data_root}/resource_cache",
    "${data_root}/operation_staging",
    "${data_root}/operations",
    "${data_root}/operation_executions",
    "${data_root}/public_data",
    "${data_root}/docker",
  ]

  if $platform == 'virtualbox' {
    file { $data_dirs:
      ensure => directory,
      owner  => $app_user,
      group  => $app_group,
      require => File[$data_root]
    }
  }

  if $platform == 'aws' {

    # https://forge.puppetlabs.com/puppetlabs/lvm
    filesystem { $data_volume_device_name:
      ensure  => present,
      fs_type => 'ext4',
      before  => File[$data_root],
    }

    mount { $data_root:
      ensure  => mounted,
      device  => $data_volume_device_name,
      fstype  => 'ext4',
      options => 'defaults',
      require => File[$data_root],
    }
    file { $data_dirs:
      ensure => directory,
      owner  => $app_user,
      group  => $app_group,
      require => Mount[$data_root]
    }
  }

  $mev_dependencies = [
    'build-essential',
    'apt-transport-https',
    'ca-certificates',
    'gnupg2',
    'zlib1g-dev',
    'libssl-dev',
    'libncurses5-dev',
    'libreadline-dev',
    'libbz2-dev',
    'libffi-dev',
    'liblzma-dev',
    'libsqlite3-dev',
    'libpq-dev',
    'nano',
    'git',
    'curl',
    'pkg-config',
    'netcat',
    'procps',
    'default-jre'
  ]
  package { $mev_dependencies: }

  file { '/usr/local/bin/wigToBigWig':
    ensure => present,
    source => 'http://hgdownload.cse.ucsc.edu/admin/exe/linux.x86_64/wigToBigWig',
    mode   => '0550',
    owner  => $app_user
  }

  file { '/usr/local/bin/bedGraphToBigWig':
    ensure => present,
    source => 'http://hgdownload.cse.ucsc.edu/admin/exe/linux.x86_64/bedGraphToBigWig',
    mode   => '0550',
    owner  => $app_user
  }

  file { '/usr/local/bin/bigWigToBedGraph':
    ensure => present,
    source => 'http://hgdownload.cse.ucsc.edu/admin/exe/linux.x86_64/bigWigToBedGraph',
    mode   => '0550',
    owner  => $app_user
  }

  class { 'rabbitmq':
    manage_python => false,
  }

  class { 'docker':
    docker_users => [$app_user],
    root_dir     => "${data_root}/docker"
  }

  contain mevapi::cloudwatch_agent
  contain mevapi::django
  contain mevapi::nextflow
  contain mevapi::nginx
  contain mevapi::postgresql
  contain mevapi::solr
  contain mevapi::supervisor

  Class['mevapi::postgresql']
  ->
  Class['mevapi::django']
  ~>
  Class['mevapi::supervisor']
  ->
  Class['mevapi::nginx']
  ->
  # Note that we put cloudwatch agent last since
  # installing/configuring earlier can lead to UID conflicts
  Class['mevapi::cloudwatch_agent']
}
