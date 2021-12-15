#!/bin/bash
set -e
export AWS_ACCESS_KEY_ID=$(aws configure get default.aws_access_key_id)
export AWS_SECRET_ACCESS_KEY=$(aws configure get default.aws_access_key_id)
export AWS_SESSION_TOKEN=$(aws configure get default.aws_access_key_id)
