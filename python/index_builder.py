#!/usr/bin/python

import csv
import os
import re
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch_dsl import connections, Index, DocType, Nested, String, GeoPoint, Integer

from pic import Constituent

import timeit
import warnings
warnings.filterwarnings('ignore')

def remove_bom(row):
    cleaned = {}
    for k, v in row.iteritems():
        if (k.find('\xef\xbb\xbf')==0):
            kk = re.sub(r'\xef\xbb\xbf', r'', k)
            cleaned[kk] = v
        else:
            cleaned[k] = v
    return cleaned

def convertWhitespace(string):
    string = string.replace('\xc2\xa0', ' ') # because WTF MSSQL Server!?
    string = string.replace('\u00a0', ' ') # because WTF MSSQL Server!?
    return string

def build_action(row, index, doc_type):
    return {
        "_index": index,
        "_type": doc_type,
        "_source": remove_bom(row)
    }

def remove_zeroes(string):
    while string.endswith("0") and len(string)>1:
        string = string[0:-1]
    return string

def compress_address(remarks):
    precision = 4
    temp = remarks.replace(" ", "").split(",")
    lat = temp[0]
    if lat.find(".") != -1:
        lat = lat.split(".")
        lat[1] = remove_zeroes(lat[1][:precision])
        temp[0] = lat[0] + "." + lat[1]
    lon = temp[1]
    if lon.find(".") != -1:
        lon = lon.split(".")
        lon[1] = remove_zeroes(lon[1][:precision])
        temp[1] = lon[0] + "." + lon[1]
    return temp

def process_csv(filename):
    basepath = os.environ['BASEPATH']
    readpath = basepath+filename
    print "loaded " + readpath
    response = open(readpath)
    reader = csv.DictReader(response)
    return reader

def generate_base_locations():
    """
    Gets all locations/heights for photographers
    Saves in latlons.txt and heights.txt
    """
    basepath = os.environ['BASEPATH']
    response = open(basepath+'address.csv')
    print "loaded " + basepath + "address.csv"
    reader = csv.DictReader(response)
    location_pattern = re.compile("(\-?\d+(\.\d+)?)\s*,\s*(\-?\d+(\.\d+)?).*")
    places = []
    heights = []
    every_row = []
    for row in reader:
        row = remove_bom(row)
        remarks = row['Remarks']
        remarks = convertWhitespace(remarks)
        if remarks == None:
            print "No remarks:" + row['ConstituentID'] + ":" + remarks
            continue
        if location_pattern.match(remarks) == None:
            if remarks != "NULL":
                print "NULL remarks:" + row['ConstituentID'] + ":" + remarks
            continue
        row['Remarks'] = remarks
        every_row.append(row)
    every_row = sorted(every_row, key=lambda d: d['BeginDate'])
    # put born first and died last always
    other_addresses = []
    born_addresses = []
    died_addresses = []
    for add in every_row:
        # put the active/biz ones
        if (add['AddressTypeID'] == '7' or add['AddressTypeID'] == '2'):
            other_addresses.append(add)
        # find born if any
        if (add['AddressTypeID'] == '5'):
            born_addresses.append(add)
        # find died if any
        if (add['AddressTypeID'] == '6'):
            died_addresses.append(add)
    for_real_sorted_every_row = []
    for_real_sorted_every_row.extend(born_addresses)
    for_real_sorted_every_row.extend(other_addresses)
    for_real_sorted_every_row.extend(died_addresses)
    for row in for_real_sorted_every_row:
        address = compress_address(row['Remarks'])
        height = 0
        if len(address) > 2:
            height = address.pop()
            heights.extend([row['ConAddressID'], height])
        address.append(row['ConstituentID'])
        address.append(row['ConAddressID'])
        if row['AddressTypeID'] != "NULL":
            address.append(row['AddressTypeID'])
        else:
            print "no type:" + row['ConstituentID']
            address.append("1")
        address.append(row['CountryID'])
        places.extend(address)
    locations = "[\"constituents\", [" + ",".join(places) + "]]"
    output = basepath+'latlons.txt'
    print "wrote " + output
    text_file = open(output, "w")
    text_file.write(locations)
    text_file.close()
    locations = "[\"heights\", [" + ",".join(heights) + "]]"
    output = basepath+'heights.txt'
    print "wrote " + output
    text_file = open(output, "w")
    text_file.write(locations)
    text_file.close()

def create_base_constituents():
    reader = process_csv('constituents.csv')
    constituents = {}
    for row in reader:
        row = remove_bom(row)
        if (row['AlphaSort'] == None):
            print "No AlphaSort in: " + row['ConstituentID']
            row['AlphaSort'] = ''
        row['nameSort'] = row['AlphaSort'].split(" ")[0]
        constituents[row['ConstituentID']] = row
    return constituents

def sort_addresses(addresses):
    sortedaddresses = []
    born = {}
    died = {}
    if len(addresses) <= 1:
        return addresses
    ## sort by date
    addresses = sorted(addresses, key=lambda d: d['BeginDate'])
    for add in addresses:
        # put the active/biz ones
        if (add['AddressTypeID'] == '7' or add['AddressTypeID'] == '2'):
            sortedaddresses.append(add)
        # find born if any
        if (add['AddressTypeID'] == '5'):
            born = add
        # find died if any
        if (add['AddressTypeID'] == '6'):
            died = add
    # prepend born
    if (born):
        sortedaddresses.insert(0,born)
    # append died
    if (died):
        sortedaddresses.append(died)
    return sortedaddresses

def process_constituents():
    """
    Consolidates all constituent data into a single dictionary. Pushes each dictionary as a document to Elastic.
    """
    start = timeit.default_timer()
    endpoint = "https://" + os.environ['ELASTIC_USER'] + ":" + os.environ['ELASTIC_PASSWORD'] + "@" + os.environ['ENDPOINT']
    constituents = create_base_constituents()
    tables = ["format","biography","address","gender","process","role","collection"]
    location_pattern = re.compile("(\-?\d+(\.\d+)?)\s*,\s*(\-?\d+(\.\d+)?)")
    for table in tables:
        reader = process_csv(table+".csv")
        for row in reader:
            row = remove_bom(row)
            if ((row['ConstituentID'] in constituents) == False):
                print "No constituent:" + row['ConstituentID']
                continue
            if ((table in constituents[row['ConstituentID']]) == False):
                constituents[row['ConstituentID']][table] = []
            cid = row['ConstituentID']
            del row['ConstituentID']
            if 'Remarks' in row:
                row['Remarks'] = convertWhitespace(row['Remarks'])
                if location_pattern.match(row['Remarks']):
                    row['Remarks'] = ",".join(compress_address(row['Remarks']))
            constituents[cid][table].append(row)
    # now sort addresses
    for index, cid in enumerate(constituents):
        if 'address' in constituents[cid]:
            constituents[cid]['address'] = sort_addresses(constituents[cid]['address'])
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
    print start
    generate_base_locations()
    end = timeit.default_timer()
    print end - start
    start = end
    process_constituents()
    end = timeit.default_timer()
    print end - start

if __name__ == "__main__":
    main()
