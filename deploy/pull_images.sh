#!/bin/bash

docker login -e $DOCKER_EMAIL -u $DOCKER_USER -p $DOCKER_PASS

docker pull brooksnoble/apartmentsearch
