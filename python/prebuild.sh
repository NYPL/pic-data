#!/bin/bash

echo ""
echo "#### STARTED TRAVIS-CI MAGIC"

echo "Creating text files"
python ./python/data_converter.py

echo ""
echo "Running indexer"
python ../python/index_builder.py

echo ""
echo "#### DEPLOY COMPLETE"