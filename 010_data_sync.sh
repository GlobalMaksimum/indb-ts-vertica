#!/usr/bin/env bash

# Send data to local s3 bucket

mc mirror data/nyc-trip lab/share/nyc-trip/

mc ls lab/share/nyc-trip