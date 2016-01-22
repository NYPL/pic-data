import csv
import re

from elasticsearch_dsl import analyzer, DocType, Object, Nested, String, Integer, Long

accent_analyzer = analyzer('accent_analyzer',
    tokenizer='standard',
    filter = ['lowercase', 'asciifolding']
)

class Constituent(DocType):
    ConstituentID = String()
    DisplayName = String(analyzer=accent_analyzer)
    DisplayDate = String()
    AlphaSort = String(
        analyzer=accent_analyzer,
        fields={'raw': String(index='not_analyzed')}
    )
    Nationality = String()
    BeginDate = String()
    EndDate = String()
    ConstituentTypeID = String()
    addressTotal = Integer()
    nameSort = String()
    TextEntry = String()

    address = Nested(
        include_in_parent=True,
        properties={
            'ConAddressID' : String(),
            'ConstituentID' : String(),
            'AddressTypeID' : String(),
            'DisplayName2' : String(),
            'StreetLine1' : String(),
            'StreetLine2' : String(),
            'StreetLine3' : String(),
            'City' : String(),
            'State' : String(),
            'CountryID' : String(),
            'BeginDate' : String(),
            'EndDate' : String(),
            'Remarks' : String()
        }
    )

    biography = Object(
        properties={
            'TermID' : String(),
            'URL' : String()
        }
    )

    collection = Object(
        properties={
            'TermID' : String(),
            'URL' : String()
        }
    )

    format = Object(
        properties={
            'TermID' : String()
        }
    )

    gender = Object(
        properties={
            'TermID' : String()
        }
    )

    process = Object(
        properties={
            'TermID' : String()
        }
    )

    role = Object(
        properties={
            'TermID' : String()
        }
    )

class Converter:

    @staticmethod
    def remove_bom(row):
        cleaned = {}
        for k, v in row.iteritems():
            if (k.find('\xef\xbb\xbf')==0):
                kk = re.sub(r'\xef\xbb\xbf', r'', k)
                cleaned[kk] = v
            else:
                cleaned[k] = v
        return cleaned

    @staticmethod
    def convert_whitespace(string):
        string = string.replace('\xc2\xa0', ' ') # because WTF MSSQL Server!?
        string = string.replace('\u00a0', ' ') # because WTF MSSQL Server!?
        return string

    @staticmethod
    def remove_zeroes(string):
        while string.endswith("0") and len(string)>1:
            string = string[0:-1]
        return string

    @staticmethod
    def compress_address(remarks):
        precision = 4
        temp = remarks.replace(" ", "").split(",")
        lat = temp[0]
        if lat.find(".") != -1:
            lat = lat.split(".")
            lat[1] = Converter.remove_zeroes(lat[1][:precision])
            temp[0] = lat[0] + "." + lat[1]
        lon = temp[1]
        if lon.find(".") != -1:
            lon = lon.split(".")
            lon[1] = Converter.remove_zeroes(lon[1][:precision])
            temp[1] = lon[0] + "." + lon[1]
        return temp

    @staticmethod
    def process_csv(filename):
        print "loaded " + filename
        response = open(filename)
        reader = csv.DictReader(response)
        return reader

    @staticmethod
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
