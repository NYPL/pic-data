#!/usr/bin/python

import csv
import os
import re
import numpy as np

from pic import Converter

import timeit
import warnings
warnings.filterwarnings('ignore')

def get_minimum_year():
    """
    Gets the minimum year in the address/constituents CSVs using only BeginDate

    Saves in minyear.txt
    """
    basepath = os.environ['BASEPATH']

    response = open(basepath+'address.csv')
    reader = csv.DictReader(response)
    years = [int(Converter.str_to_float(item['BeginDate'])) for item in reader if item['BeginDate'] != '']
    nozeros = [number for number in years if (number != 0 and number != -1)]
    minyeara = np.amin(nozeros)
    print "\n\nMinimum year in address is: " + str(minyeara)

    response = open(basepath+'constituents.csv')
    reader = csv.DictReader(response)
    years = [int(Converter.str_to_float(item['BeginDate'])) for item in reader if item['BeginDate'] != '']
    nozeros = [number for number in years if (number != 0 and number != -1)]
    minyearb = np.amin(nozeros)
    print "\n\nMinimum year in constituents is: " + str(minyearb)

    minyear = min(minyeara, minyearb)

    output = basepath+'minyear.txt'
    print "\nwrote " + output
    text_file = open(output, "w")
    text_file.write(str(minyear))
    text_file.close()

def main():
    start = timeit.default_timer()
    print start
    get_minimum_year()
    end = timeit.default_timer()
    print end - start

if __name__ == "__main__":
    main()
