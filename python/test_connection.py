#!/usr/bin/python

import os
import re

from dotenv import load_dotenv

load_dotenv()

from elasticsearch import Elasticsearch
from elasticsearch_dsl import connections, Index

from pic import Constituent, Address
from index_builder import IndexBuilder

import warnings
warnings.filterwarnings('ignore')

def main():
    endpoint = IndexBuilder.create_endpoint()
    if endpoint == "":
        print("\n\nIMPORT ABORTED!")
        return
    print("\n\nTrying index creation")
    IndexBuilder.create_indices(endpoint)
    print("\n\nTrying ES instantiation")
    constituents = IndexBuilder.process_constituents(endpoint=endpoint, test=True)

if __name__ == "__main__":
    main()
