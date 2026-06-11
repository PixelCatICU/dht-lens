#!/bin/bash

# Ensure CapRover CLI is installed
if ! command -v caprover &> /dev/null; then
    echo "CapRover CLI is not installed. Please install it first:"
    echo "npm install -g caprover"
    exit 1
fi

APP_NAME="dht-lens"

# Deploy to CapRover
echo "Deploying $APP_NAME to CapRover..."
caprover deploy --appName $APP_NAME --defaultName $APP_NAME -d ./
