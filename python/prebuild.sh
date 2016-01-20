#!/bin/bash

echo ""
echo "#### STARTED TRAVIS-CI MAGIC"

# export REPO_URL="https://$GH_TOKEN@github.com/$GH_REPO.git"

# git config --global user.name "travis-bot"
# git config --global user.email "travis"

echo "Creating text files"
python ./python/data_converter.py

echo ""
echo "Running indexer"
python ../python/index_builder.py

echo ""
echo "#### DEPLOY COMPLETE"