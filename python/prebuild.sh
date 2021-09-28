#!/bin/bash

echo ""
echo "#### STARTED TRAVIS-CI MAGIC"

# echo "Testing connection"
# python ./python/test_connection.py

echo "Creating text files"
python ./python/data_converter.py

echo "Getting minimum year"
python ./python/minimum_year.py

echo ""
echo "Running indexer"
python ./python/index_builder.py

echo ""
echo "#### INDEXING COMPLETE"