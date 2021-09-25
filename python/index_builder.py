#!/usr/bin/python

import csv
import os
import re
import json

from dotenv import load_dotenv

load_dotenv()

from elasticsearch import Elasticsearch, serializer, compat, exceptions
from elasticsearch import helpers
from elasticsearch_dsl import connections, Index
from pic import Constituent, Address, Converter

import timeit
import warnings
warnings.filterwarnings('ignore')

class JSONSerializerPython2(serializer.JSONSerializer):
    """Override elasticsearch library serializer to ensure it encodes utf characters during json dump.
    See original at: https://github.com/elastic/elasticsearch-py/blob/master/elasticsearch/serializer.py#L42
    A description of how ensure_ascii encodes unicode characters to ensure they can be sent across the wire
    as ascii can be found here: https://docs.python.org/2/library/json.html#basic-usage

    Taken from: https://github.com/elastic/elasticsearch-py/issues/374#issue-139119498
    """
    def dumps(self, data):
        # don't serialize strings
        if isinstance(data, compat.string_types):
            return data
        try:
            return json.dumps(data, default=self.default, ensure_ascii=True)
        except (ValueError, TypeError) as e:
            raise exceptions.SerializationError(data, e)

class IndexBuilder:
    @staticmethod
    def build_action(row, index, document):
        """
        Creates an ES action for indexing
        """
        cleaned = Converter.remove_bom(row)
        if not "id" in cleaned:
            print("NO ID!!!1!")
            print(cleaned)
        cleaned["constituent_address"] = document
        if (document == "address"):
            cleaned["constituent_address"] = {"name": document, "parent": cleaned['ConstituentID']}
        action = {
            "_index": index,
            "_source": cleaned,
            "_routing": 1,
            "_id": cleaned["id"],
        }
        return action

    @staticmethod
    def create_base_constituents(test):
        """
        Builds the raw skeleton of a constituent from constituents.csv
        """
        if (test):
            csv='test-constituents.csv'
        else:
            csv='constituents.csv'
        reader = Converter.process_csv(os.environ['BASEPATH'] + csv)
        constituents = {}
        for row in reader:
            row = Converter.remove_bom(row)
            if (row['AlphaSort'] == None):
                print("No AlphaSort in: " + row['ConstituentID'])
                row['AlphaSort'] = ''
            row['nameSort'] = row['AlphaSort'].split(" ")[0]
            row['id'] = row['ConstituentID']
            constituents[row['ConstituentID']] = row
        return constituents

    @staticmethod
    def create_constituent(data):
        """
        Creates a constituent in ES with data
        """
        return IndexBuilder.build_action(data, 'pic', 'constituent')

    @staticmethod
    def create_indices(endpoint):
        """
        Creates constituent and address indices in PIC
        """
        connections.connections.create_connection(hosts=[endpoint], timeout=360, max_retries=10, retry_on_timeout=True)
        pic_index = Index('pic')
        pic_index.document(Constituent)
        pic_index.document(Address)
        pic_index.delete(ignore=404)

        pic_index.settings(
            number_of_shards=5,
            number_of_replicas=2
        )
        pic_index.create()

    @staticmethod
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

    @staticmethod
    def process_constituents(endpoint, test=False):
        """
        Consolidates all constituent data into a single dictionary. Pushes each dictionary as a document to Elastic.
        """
        start = timeit.default_timer()
        constituents = IndexBuilder.create_base_constituents(test)
        counter = 0
        tables = ["format","biography","address","gender","process","role","collection"]
        joins = ["formats","biographies",["addresstypes","countries"],"genders","processes","roles","collections"]
        location_pattern = re.compile("(\-?\d+(\.\d+)?)\s*,\s*(\-?\d+(\.\d+)?)")
        for table in tables:
            if (test):
                filename = "test-" + table + ".csv"
            else:
                filename = table + ".csv"
            reader = Converter.process_csv(os.environ['BASEPATH'] + filename)
            if type(joins[counter]) is str:
                joindata = IndexBuilder.get_join_data(os.environ['BASEPATH'] + joins[counter] + ".csv")
            else:
                j0 = IndexBuilder.get_join_data(os.environ['BASEPATH'] + joins[counter][0] + ".csv")
                j1 = IndexBuilder.get_join_data(os.environ['BASEPATH'] + joins[counter][1] + ".csv")
                joindata = [j0,j1]
            for row in reader:
                row = Converter.remove_bom(row)
                if ((row['ConstituentID'] in constituents) == False):
                    print("No constituent:" + row['ConstituentID'])
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
                        print("No " + table + ":" + row['TermID'])
                    else:
                        row['Term'] = joindata[row['TermID']]
                if 'AddressTypeID' in row:
                    if ((row['AddressTypeID'] in joindata[0]) == False):
                        print("No " + joins[counter][0] + ":" + row['AddressTypeID'])
                        print(joindata[0])
                    else:
                        row['AddressType'] = joindata[0][row['AddressTypeID']]
                    if ((row['CountryID'] in joindata[1]) == False):
                        print("No " + joins[counter][1] + ":" + row['CountryID'])
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
        print("\n\nProcessed CSVs in " + str(end - start) + " seconds\n")
        print("\n\nPreparing indexing actions...\n")
        start = timeit.default_timer()
        # now on to elastic (index already created)
        actions = []
        addresslist = []
        for index, cid in enumerate(constituents):
            addresses = []
            if 'address' in constituents[cid]:
                # sort addresses
                addresses = Converter.sort_addresses(constituents[cid]['address'])
                constituents[cid]['addressTotal'] = len(constituents[cid]['address'])
                del constituents[cid]['address']
                # put the addresses
                addresslist = addresslist + addresses
            # put the constituent in there
            actions.append(IndexBuilder.create_constituent(constituents[cid]))
        end = timeit.default_timer()
        print("\n\nActions prepared in " + str((end - start)/60) + " minutes\n")
        print("\n\nIndexing...\n")
        IndexBuilder.create_indices(endpoint)
        es = Elasticsearch([endpoint], timeout=360, max_retries=10, retry_on_timeout=True, serializer=JSONSerializerPython2())
        # split the actions into batches of 10k
        print("  Constituents...")
        index = 0
        n = 10000
        splitactions = IndexBuilder.split_list(actions, n)
        for actionchunk in splitactions:
            print("    actions " + str(index*n) + " to " + str((index+1)*n))
            index = index + 1
            helpers.bulk(es, actionchunk)
        print("  Addesses...")
        index = 0
        splitaddresses = IndexBuilder.split_list(addresslist, n)
        for addresschunk in splitaddresses:
            print("    actions " + str(index*n) + " to " + str((index+1)*n))
            index = index + 1
            helpers.bulk(es, [IndexBuilder.build_action(value, 'pic', 'address') for value in addresschunk])
        return constituents

    @staticmethod
    def split_list(original_list, chunksize = 10000):
        return [original_list[i:i+chunksize] for i in range(0, len(original_list), chunksize-1)]

    @staticmethod
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
        # print(protocol)
        try:
            credentials = ""
            if os.environ['ELASTIC_USER'] != "" and os.environ['ELASTIC_PASSWORD'] != "":
                credentials = os.environ['ELASTIC_USER'] + ":" + os.environ['ELASTIC_PASSWORD'] + "@"
            endpoint = protocol + credentials + os.environ['ENDPOINT']
            return endpoint
        except KeyError:
            print("\n\nNo environment variables set! (possible pull request)\n")
            return ""

def main():
    start = timeit.default_timer()
    endpoint = IndexBuilder.create_endpoint()
    if (endpoint == ""):
        print("\n\nIMPORT ABORTED!")
        return
    constituents = IndexBuilder.process_constituents(endpoint)
    end = timeit.default_timer()
    print("Done in " + str((end - start)/60) + " minutes")

if __name__ == "__main__":
    main()
