#!/usr/bin/python

import csv
import os
import re

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


def main():
    start = timeit.default_timer()
    print start
    generate_base_locations()
    end = timeit.default_timer()
    print end - start

if __name__ == "__main__":
    main()
