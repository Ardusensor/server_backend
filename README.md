Ardusensor server component

[![Build Status](https://travis-ci.org/Ardusensor/server_backend.svg?branch=master)](https://travis-ci.org/Ardusensor/server_backend)

How to install
--------------

* Install Go

https://golang.org/doc/install

* Install Redis

http://redis.io/download

How to run
----------

* Start Redis

``` console
redis-server
```

* Start server

``` console
make
./backend
```

How to deploy
-------------

* Install Redis on server

* Copy the built binary (backend) to server, then run it

How to configure
----------------

To see possible runtime options,

``` console
./backend --help
```


