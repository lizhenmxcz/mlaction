import csv
import json
import netaddr

def csvToJson( inFile, outFile ):

    #Note this reads the first line as the keys we can add specific keys with:
    csvFile = file(inFile,'rb')
    reader = csv.reader(csvFile)
    items = []
    nodes = []
    links = []
    for line in reader:
        items.append(line[0]);
        items.append(line[1]);
    print len(items)
    items = list(set(items))
    print len(items)
    for item in items:
        dic={}
        dic['name']=str(netaddr.IPAddress(item)) 
        nodes.append(dic)
    csvFile.seek(0);
    for line in reader:
        linkitem={}
        linkitem['source']=str(netaddr.IPAddress(line[0]))
        linkitem['target']=str(netaddr.IPAddress(line[1]))
        linkitem['weight']=int(line[2])
        links.append(linkitem)
    graph={}
    graph['nodes']=nodes
    graph['links']=links
    jsonFile = file(outFile,'wb')
    encodedjson = json.dumps(graph)
    jsonFile.write(encodedjson)

        



if __name__ == "__main__":
     import argparse

     parser = argparse.ArgumentParser()
     parser.add_argument('inFile', nargs=1, help="Choose the in file to use")
     parser.add_argument('outFile', nargs=1, help="Choose the out file to use")
     args = parser.parse_args()
     csvToJson( args.inFile[0] , args.outFile[0] );
