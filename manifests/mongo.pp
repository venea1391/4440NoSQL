package { 
	'mongodb' :	ensure => installed,
}
package {
	'git' : ensure => installed
}
service {
	'mongodb' : ensure => running, enable => true, require => Package['mongodb']
}
exec { 
	'allow remote mongo connections':
 		command => "/usr/bin/sudo sed -i 's/bind_ip = 127.0.0.1/bind_ip = 0.0.0.0/g' /etc/mongodb.conf",
   		notify  => Service['mongodb'],
  		onlyif  => '/bin/grep -qx  "bind_ip = 127.0.0.1" /etc/mongodb.conf',
}
exec {
	'download and run metrics':
		command => '/usr/bin/git init; /usr/bin/git fetch https://github.com/venea1391/4440NoSQL.git',
		require => [Exec['allow remote mongo connections'], Package['git']]
}
