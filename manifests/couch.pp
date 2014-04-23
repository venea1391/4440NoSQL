exec {
	'update-apt' :
	command => '/usr/bin/apt-get update'
}
package {
	'couchdb' : ensure => installed, require => exec['update-apt']
}
service {
	'couchdb' : ensure => running, enable => true, require => Package['couchdb']
}
exec { 
		'allow remote couch connections':
		 		command => "/usr/bin/sudo sed -i 's/;bind_address = 127.0.0.1/bind_address = 0.0.0.0/g' /etc/couchdb/local.ini",
				   		notify  => Service['couchdb'],
						  		onlyif  => '/bin/grep -qx  ";bind_address = 127.0.0.1" /etc/couchdb/local.ini',
}
