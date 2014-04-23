package { 
	'mongodb' :	ensure => installed,
}
service {
	'mongodb' : ensure => running, enable => true, require => Package['mongodb']
}
exec { 'allow remote mongo connections':
  command => "/usr/bin/sudo sed -i 's/bind_ip = 127.0.0.1/bind_ip = 0.0.0.0/g' /etc/mongodb.conf",
    notify  => Service['mongodb'],
	  onlyif  => '/bin/grep -qx  "bind_ip = 127.0.0.1" /etc/mongodb.conf',
}
