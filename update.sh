#!/bin/bash

pushd ~/Desktop/DoCoMo/s3/
date
aws s3 sync s3://mobaku-delivery-user002 .
date
popd

python update.py
date
