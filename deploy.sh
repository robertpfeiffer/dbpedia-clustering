#!/bin/bash

# config
user=hadoop03
server=hadoop09ws10

# commands
ant jar || exit -1
git push
scp dist/clustering.jar $user@$server:~