#!/usr/bin/env bash

if [[ $# -ne 2 ]]; then
 echo "Usage: $0 <name> <arch>"
 exit 1
fi

folder=${1}
arch=${2}

docker run --name taskr --shm-size=1024M --privileged -td "registry.gitlab.huaweirc.ch/zrc-von-neumann-lab/runtime-system-innovations/taskr/${folder}-${arch}:latest" bash 
