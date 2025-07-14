# distribution-engine
Indexing for Hive Engine Comments Smart Contract

## Installation of packages for Ubuntu 18.04

```
sudo apt-get install postgresql postgresql-contrib python3-pip libicu-dev build-essential libssl-dev python3-dev git

# versioned python packages
sudo apt-get install python3.8-dev
```

## Server

```
sudo apt install nginx ufw php7.2-fpm php7.2-pgsql
```
adminer (for UI access to manage postgres DB)
```
sudo mkdir /usr/share/adminer
sudo wget "https://www.adminer.org/latest.php" -O /usr/share/adminer/latest.php
sudo ln -s /usr/share/adminer/latest.php /usr/share/adminer/adminer.php
sudo ln -s /usr/share/adminer/adminer.php /var/www/html
```

## Installation of python packages

**General Warning**: Be aware of which pip version is used. If straddling versions, you may have pip3, pip3.6, pip3.7. or use python3.x -m pip.

(Block streaming is using scot user)
```
sudo apt-get install -y python3-setuptools
sudo apt-get install -y python3.8-dev
python3.8 -m pip install wheel nectar dataset psycopg2-binary nectarengine base36 python-dateutil
```

(API on machine is using root to run gunicorn)
```
sudo su
python3.8 -m pip install gunicorn flask flask-cors flask-compress flask-caching prettytable pytz 
python3.8 -m pip install wheel nectar dataset psycopg2-binary secp256k1prp nectarengine base36 sqltap simplejson
python3 setup.py install
```

NOTE: In some cases, need --user setting for the module to be visible to the api server (if running official setup),
e.g. `sudo su; python3.8 -m pip install --user flask-compress flask-caching`

Note the config cache directory (config.json), make sure that directory is set up and accessible.

## /mem
```
sudo cp /etc/fstab /etc/fstab.orig
sudo mkdir /mem
echo 'tmpfs       /mem tmpfs defaults,size=64m,mode=1777,noatime,comment=for-gunicorn 0 0' | sudo tee -a /etc/fstab
sudo mount /mem
```

## Create engine user
```
adduser engine
```

## Setup of the postgresql database

Set a password and a user for the postgres database:

```
su postgres
psql -c "\password"
createdb engine
```

## Prepare the postgres database
```
psql -d engine -a -f sql/engine.sql
```

## Config file for accessing the database and engine rpc
A `config.json` file must be stored in the main directory and in the homepage directory where the `app.py` file is.
```
{
        "engine_api": "https://api2.hive-engine.com/rpc/",
        "engine_id": "ssc-mainnet-hive",
        "apiCacheDir": "/tmp/scotcache"
        "databaseConnector": "postgresql://postgres:password@localhost/engine",
}
```

Also create /tmp/scotcache

```
mkdir /tmp/scotcache
```

## Running the scripts

When running for the first time, set the `last_streamed_block` of the HIVED configuration entry
and the `last_engine_streamed_block` of the `ENGINE_SIDECHAIN` configuration entry
to the hive block and hive engine sidechain block, respectively, where your SMT was introduced.

Run the following with your process manager of choice (e.g. pm2)
```
./run-engine-sc.sh
./run-engine.sh
(dev) ./run-api-server.sh
(prod) ./run-prod-api-server.sh
```
