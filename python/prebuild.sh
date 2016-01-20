#!/bin/bash

echo ""
echo "#### STARTED TRAVIS-CI MAGIC"

export REPO_URL="https://$GH_TOKEN@github.com/$GH_REPO.git"

git config --global user.name "travis-bot"
git config --global user.email "travis"

#####

# create text files
echo "Creating text files"
python ./python/data_converter.py

#####

echo ""
echo "Clone the app repo"
git clone $REPO_URL _cloned

cd _cloned

echo ""
echo "Add the new files"
echo "Before:"
ls ./public/assets/csv
cp ../csv/*.txt ./public/assets/csv
cp ../csv/*.csv ./public/assets/csv
echo ""
echo "After:"
ls ./public/assets/csv

echo ""
echo "Status"
git status

echo ""
echo "Adding new files"
git add public/assets/csv

git commit -m ":rocket: new deploy from travis-ci"

# echo ""
# echo "Running indexer"
# cd ..
# python ./python/index_builder.py

echo ""
echo "Push to update app repo"

git push origin master

echo ""
echo "#### DEPLOY COMPLETE"