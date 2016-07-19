import geoip2.database
import csv
import netaddr


def translate( inFile, outFile ):
    csvFile = file(inFile,'rb')
    reader = csv.reader(csvFile)
    geoReader = geoip2.database.Reader('data/GeoLite2-City.mmdb')
    outfile = file(outFile,'wb')
    writer = csv.writer(outfile)
    for line in reader:
    	row = []
    	ip = netaddr.IPAddress(line[0])
    	row.append(str(ip))
    	row.append(line[1])
        try:
            response = geoReader.city(ip)     
        except Exception, e:
            print ip
            continue   	
    	row.append(response.location.longitude)
    	row.append(response.location.latitude)
    	writer.writerow(row)
    geoReader.close()
    csvFile.close()
    outfile.close()
if __name__ == "__main__":
     import argparse

     parser = argparse.ArgumentParser()
     parser.add_argument('inFile', nargs=1, help="Choose the in file to use")
     parser.add_argument('outFile', nargs=1, help="Choose the out file to use")
     args = parser.parse_args()
     translate( args.inFile[0] , args.outFile[0] );





