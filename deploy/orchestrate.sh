#!/bin/bash

# Remove all existing containers (stopped and running)
docker ps -qa | xargs docker rm -f

docker run -d --name mongo mongo
sleep 5s
docker run -d --name web --link eventstore:eventstore --link mongo:mongo -e "MongoConnectionString=mongodb://mongo" -p 80:8083 brooksnoble/apartmentsearch
