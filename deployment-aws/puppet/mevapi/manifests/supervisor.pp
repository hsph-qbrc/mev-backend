class mevapi::supervisor () {
  $conf_dir = '/etc/supervisor'

  package { 'supervisor': }
  ->
  file {
    default:
      ensure => file,;
    "${conf_dir}/supervisord.conf":
      content => epp('mevapi/supervisor/supervisord.conf.epp'),;
    "${conf_dir}/conf.d/gunicorn.conf":
      content => epp('mevapi/supervisor/gunicorn.conf.epp'),;
    "${conf_dir}/conf.d/celery_beat.conf":
      content => epp('mevapi/supervisor/celery_beat.conf.epp'),;
    "${conf_dir}/conf.d/celery_worker.conf":
      content => epp('mevapi/supervisor/celery_worker.conf.epp'),;
  }
  ~>
  service { 'supervisor':
    ensure => running,
    enable => true,
  }
}
