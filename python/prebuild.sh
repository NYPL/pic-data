#!/bin/bash

echo ""
echo "#### STARTED TRAVIS-CI MAGIC"

if [ "$1" = "test" ]
then
    echo "Testing connection"
    python ./python/test_connection.py
fi

if [ -z $1 ]
then
    echo "Creating text files"
    python ./python/data_converter.py

    echo "Getting minimum year"
    python ./python/minimum_year.py

    echo ""
    echo "Running indexer"
    python ./python/index_builder.py
fi

echo ""
echo "#### INDEXING COMPLETE"