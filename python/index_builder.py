#!/usr/bin/python

import csv
import os
import re

from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch_dsl import connections, Index, DocType, Nested, String, GeoPoint, Integer
from pic import Constituent, Address, Converter

import timeit
import warnings
warnings.filterwarnings('ignore')

def build_action(row, index, doc_type):
    """
    Creates an ES action for indexing
    """
    cleaned = Converter.remove_bom(row)
    if not "id" in cleaned:
        print "NO ID!!!1!"
        print cleaned
    action = {
        "_index": index,
        "_type": doc_type,
        "_source": cleaned,
        "_id": cleaned["id"]
    }
    if doc_type == 'address':
        action["_parent"] = cleaned['ConstituentID']
        action["_routing"] = cleaned['ConstituentID']
    return action

def create_base_constituents():
    """
    Builds the raw skeleton of a constituent from constituents.csv
    """
    reader = Converter.process_csv(os.environ['BASEPATH'] + 'constituents.csv')
    constituents = {}
    for row in reader:
        row = Converter.remove_bom(row)
        if (row['AlphaSort'] == None):
            print "No AlphaSort in: " + row['ConstituentID']
            row['AlphaSort'] = ''
        row['nameSort'] = row['AlphaSort'].split(" ")[0]
        row['id'] = row['ConstituentID']
        constituents[row['ConstituentID']] = row
    return constituents

def create_constituent(data):
    """
    Creates a constituent in ES with data
    """
    return build_action(data, 'pic', 'constituent')

def create_indices(endpoint):
    """
    Creates constituent and address indices in PIC
    """
    connections.connections.create_connection(hosts=[endpoint], timeout=360, max_retries=10, retry_on_timeout=True)
    pic_index = Index('pic')
    pic_index.doc_type(Constituent)
    pic_index.doc_type(Address)
    pic_index.delete(ignore=404)

    pic_index.settings(
        number_of_shards=5,
        number_of_replicas=2
    )
    pic_index.create()


def get_join_data(filepath):
    """
    To denormalize CSV data term IDs
    """
    reader = Converter.process_csv(filepath)
    data = {}
    for row in reader:
        row = Converter.remove_bom(row)
        tmp = []
        for k in row:
            tmp.append(row[k])
        if tmp[1].isdigit():
            data[tmp[1]] = tmp[0]
        else:
            data[tmp[0]] = tmp[1]
    return data


def process_constituents(endpoint):
    """
    Consolidates all constituent data into a single dictionary. Pushes each dictionary as a document to Elastic.
    """
    start = timeit.default_timer()
    constituents = create_base_constituents()
    counter = 0
    tables = ["format","biography","address","gender","process","role","collection"]
    joins = ["formats","biographies",["addresstypes","countries"],"genders","processes","roles","collections"]
    location_pattern = re.compile("(\-?\d+(\.\d+)?)\s*,\s*(\-?\d+(\.\d+)?)")
    for table in tables:
        reader = Converter.process_csv(os.environ['BASEPATH'] + table + ".csv")
        if type(joins[counter]) is str:
            joindata = get_join_data(os.environ['BASEPATH'] + joins[counter] + ".csv")
        else:
            j0 = get_join_data(os.environ['BASEPATH'] + joins[counter][0] + ".csv")
            j1 = get_join_data(os.environ['BASEPATH'] + joins[counter][1] + ".csv")
            joindata = [j0,j1]
        for row in reader:
            row = Converter.remove_bom(row)
            if ((row['ConstituentID'] in constituents) == False):
                print "No constituent:" + row['ConstituentID']
                continue
            if ((table in constituents[row['ConstituentID']]) == False):
                constituents[row['ConstituentID']][table] = []
            cid = row['ConstituentID']
            if not 'ConAddressID' in row:
                del row['ConstituentID']
            else:
                row['id'] = row['ConAddressID']
            # add the value of the term id
            if 'TermID' in row:
                if ((row['TermID'] in joindata) == False):
                    print "No " + table + ":" + row['TermID']
                else:
                    row['Term'] = joindata[row['TermID']]
            if 'AddressTypeID' in row:
                if ((row['AddressTypeID'] in joindata[0]) == False):
                    print "No " + joins[counter][0] + ":" + row['AddressTypeID']
                    print joindata[0]
                else:
                    row['AddressType'] = joindata[0][row['AddressTypeID']]
                if ((row['CountryID'] in joindata[1]) == False):
                    print "No " + joins[counter][1] + ":" + row['CountryID']
                else:
                    row['Country'] = joindata[1][row['CountryID']]
            if 'Remarks' in row:
                row['Remarks'] = Converter.convert_whitespace(row['Remarks'])
                if location_pattern.match(row['Remarks']):
                    latlon = Converter.compress_address(row['Remarks'])
                    row['Remarks'] = ",".join(latlon)
                    row['Location'] = { "lat" : float(latlon[0]), "lon" : float(latlon[1]) }
            constituents[cid][table].append(row)
        counter = counter + 1
    end = timeit.default_timer()
    print "\n\nProcessed CSVs in " + str(end - start) + " seconds\n"
    print "\n\nPreparing indexing actions...\n"
    start = timeit.default_timer()
    # now on to elastic (index already created)
    actions = []
    for index, cid in enumerate(constituents):
        addresses = []
        if 'address' in constituents[cid]:
            # sort addresses
            addresses = Converter.sort_addresses(constituents[cid]['address'])
            constituents[cid]['addressTotal'] = len(constituents[cid]['address'])
            del constituents[cid]['address']
        # put the constituent in there
        actions.append(create_constituent(constituents[cid]))
        if len(addresses) > 0:
            # put the addresses
            actions = actions + ([build_action(value, 'pic', 'address') for value in addresses])
    end = timeit.default_timer()
    print "\n\nActions prepared in " + str((end - start)/60) + " minutes\n"
    print "\n\nIndexing...\n"
    create_indices(endpoint)
    es = Elasticsearch([endpoint], timeout=360, max_retries=10, retry_on_timeout=True)
    helpers.bulk(es, actions)
    return constituents

def create_endpoint():
    """
    Builds the ES endpoint string

    Returns empty if unable (no environment vars present)
    """
    protocol = "https://"
    try:
        protocol = os.environ['ELASTIC_PROTOCOL']
    except KeyError:
        pass
    # print protocol
    try:
        endpoint = protocol + os.environ['ELASTIC_USER'] + ":" + os.environ['ELASTIC_PASSWORD'] + "@" + os.environ['ENDPOINT']
        return endpoint
    except KeyError:
        print "\n\nNo environment variables set! (possible pull request)\n"
        return ""

def main():
    start = timeit.default_timer()
    endpoint = create_endpoint()
    if endpoint == "":
        print "\n\nIMPORT ABORTED!"
        return
    constituents = process_constituents(endpoint)
    end = timeit.default_timer()
    print "Done in " + str((end - start)/60) + " minutes"

if __name__ == "__main__":
    main()
