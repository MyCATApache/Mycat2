#!/bin/bash
cp ../../../target/mycat2*linux.tar.gz ./
docker build -t mycat2:latest .
rm -rf *.tar.gz