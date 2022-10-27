#! /bin/bash
#hadoop fs -chmod -R 777 /user

docker_registry=192.168.137.215:5000

docker pull $docker_registry/eurlanda/server:2.2.4.0-SNAPSHOT

docker tag $docker_registry/eurlanda/server:2.2.4.0-SNAPSHOT  eurlanda/server:2.2.4.0

docker pull $docker_registry/eurlanda/engine:2.2.4.0-SNAPSHOT

docker tag $docker_registry/eurlanda/engine:2.2.4.0-SNAPSHOT eurlanda/engine:2.2.4.0
