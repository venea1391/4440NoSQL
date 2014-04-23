/usr/bin/wget http://download.redis.io/releases/redis-2.8.8.tar.gz
/bin/tar xzf redis-2.8.8.tar.gz
cd redis-2.8.8
/usr/bin/sudo sed -i 's/# bind 127.0.0.1/bind 0.0.0.0' /vagrant/redis-2.8.8/redis.conf
make
/vagrant/redis-2.8.8/src/redis-server
sh get_metrics_code.sh
