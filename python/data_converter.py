#!/usr/bin/python

import csv
import os
import re

from dotenv import load_dotenv

load_dotenv()

from pic import Converter

import timeit
import warnings
warnings.filterwarnings('ignore')

def generate_base_locations():
    """
    Gets all locations/heights for photographers
    Saves in latlons.txt and heights.txt
    """
    basepath = os.environ['BASEPATH']
    response = open(basepath+'address.csv')
    print("\n\nloaded " + basepath + "address.csv")
    reader = csv.DictReader(response)
    location_pattern = re.compile("(\-?\d+(\.\d+)?)\s*,\s*(\-?\d+(\.\d+)?).*")
    places = []
    heights = []
    every_row = []
    for row in reader:
        row = Converter.remove_bom(row)
        remarks = row['Remarks']
        remarks = Converter.convert_whitespace(remarks)
        if remarks == None:
            print("No remarks:" + row['ConstituentID'] + ":" + remarks)
            continue
        if location_pattern.match(remarks) == None:
            if remarks != "NULL":
                print("NULL remarks:" + row['ConstituentID'] + ":" + remarks)
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
        address = Converter.compress_address(row['Remarks'])
        height = 0
        if len(address) > 2:
            height = address.pop()
            heights.extend([row['ConAddressID'], height])
        address.append(row['ConstituentID'])
        address.append(row['ConAddressID'])
        if row['AddressTypeID'] != "NULL":
            address.append(row['AddressTypeID'])
        else:
            print("no type:" + row['ConstituentID'])
            address.append("1")
        address.append(row['CountryID'])
        places.extend(address)
    locations = "[\"constituents\", [" + ",".join(places) + "]]"
    output = basepath+'latlons.txt'
    print("\nwrote " + output)
    text_file = open(output, "w")
    text_file.write(locations)
    text_file.close()
    locations = "[\"heights\", [" + ",".join(heights) + "]]"
    output = basepath+'heights.txt'
    print("\nwrote " + output)
    text_file = open(output, "w")
    text_file.write(locations)
    text_file.close()

def main():
    start = timeit.default_timer()
    print("started at: %f" % start)
    generate_base_locations()
    end = timeit.default_timer()
    print(end - start)

if __name__ == "__main__":
    main()
