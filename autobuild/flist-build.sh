#!/bin/bash
set -ex

apt-get update
apt-get install wget build-essential -y

# make output directory
ARCHIVE=/tmp/archives
FLIST=/tmp/flist
mkdir -p $ARCHIVE
mkdir -p $FLIST/bin/zerostor_meta
mkdir -p $FLIST/tmp

# install go
wget https://dl.google.com/go/go1.10.linux-amd64.tar.gz
tar -C /usr/local -xzf go1.10.linux-amd64.tar.gz
mkdir -p /root/go
export GOPATH=/root/go
export PATH=$PATH:/usr/local/go/bin:$GOPATH/go/bin

# install minio
mkdir -p  /root/go/src/github.com/minio
mv /minio /root/go/src/github.com/minio/minio
pushd /root/go/src/github.com/minio/minio
make build
mv minio $FLIST/bin
popd

# install restic
pushd /tmp
wget https://github.com/restic/restic/releases/download/v0.8.3/restic_0.8.3_linux_amd64.bz2
bzip2 -d restic_0.8.3_linux_amd64.bz2
mv restic_0.8.3_linux_amd64 $FLIST/bin/restic
popd

# make sure binary is executable
chmod +x $FLIST/bin/*


tar -czf "/tmp/archives/minio.tar.gz" -C $FLIST .
