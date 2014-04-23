package {
	'wget' : ensure => installed
}
package {
	'make' : ensure => installed
}
exec {
	'fetch-make-redis':
		command => "/bin/sh /vagrant/install_redis.sh",
		require => [Package['wget'], Package['make']]
}
