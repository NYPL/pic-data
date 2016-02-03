#!/usr/bin/python

import csv
import os
import re
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch_dsl import connections, Index, DocType, Nested, String, GeoPoint, Integer

from pic import Constituent, Converter

import timeit
import warnings
warnings.filterwarnings('ignore')

def build_action(row, index, doc_type):
    return {
        "_index": index,
        "_type": doc_type,
        "_source": Converter.remove_bom(row)
    }

def create_base_constituents():
    reader = Converter.process_csv(os.environ['BASEPATH'] + 'constituents.csv')
    constituents = {}
    for row in reader:
        row = Converter.remove_bom(row)
        if (row['AlphaSort'] == None):
            print "No AlphaSort in: " + row['ConstituentID']
            row['AlphaSort'] = ''
        row['nameSort'] = row['AlphaSort'].split(" ")[0]
        constituents[row['ConstituentID']] = row
    return constituents

def get_join_data(filepath):
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


def process_constituents():
    """
    Consolidates all constituent data into a single dictionary. Pushes each dictionary as a document to Elastic.
    """
    start = timeit.default_timer()
    endpoint = "https://" + os.environ['ELASTIC_USER'] + ":" + os.environ['ELASTIC_PASSWORD'] + "@" + os.environ['ENDPOINT']
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
            del row['ConstituentID']
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
                    row['Location'] = { "lat" : latlon[0], "lon" : latlon[1] }
            constituents[cid][table].append(row)
        counter = counter + 1
    # now sort addresses
    for index, cid in enumerate(constituents):
        if 'address' in constituents[cid]:
            constituents[cid]['address'] = Converter.sort_addresses(constituents[cid]['address'])
            constituents[cid]['addressTotal'] = len(constituents[cid]['address'])
    end = timeit.default_timer()
    print "Processed CSVs in " + str(end - start) + " seconds"
    print "Indexing..."
    # now on to elastic
    index = 'pic'
    document_type = 'constituent'
    connections.connections.create_connection(hosts=[endpoint], timeout=240)
    myindex = Index(index)
    myindex.doc_type(Constituent)
    try:
        myindex.delete()
    except:
        pass
    # try:
    #     es.indices.delete_mapping(index=index, doc_type=document_type)
    # except:
    #     pass
    myindex.settings(
        number_of_shards=5,
        number_of_replicas=2
    )
    myindex.create()
    actions = [build_action(value, index, document_type) for key, value in constituents.iteritems()]
    es = Elasticsearch([endpoint])
    helpers.bulk(es, actions)
    return constituents


def main():
    start = timeit.default_timer()
    constituents = process_constituents()
    end = timeit.default_timer()
    print end - start

if __name__ == "__main__":
    main()
