#FileName: PortScan.py
#find the number of port and the number of dip for every sourceip 
from impala.dbapi import connect
import datetime
import time
from IPy import IP
from subprocess import Popen, PIPE
import json

def kerberosAuthenticate():
    kinit_cmd = '/usr/bin/kinit'
    kinit = Popen([kinit_cmd, 'venus@VENUS_HADOOP.COM'], stdin=PIPE, stdout=PIPE, stderr=PIPE)
    kinit.stdin.write('%s\n' % '123456')
    kinit.wait()
    print 'authenticate success.\n'
####### get the Network Address of a IP Address #######
def getIPNetAddr(iIPAddr):
    #for the reserved IP address, eg: 10.0.0.0-10.255.255.255; 172.16.0.0-172.31.255.255; 192.168.0.0-192.168.255.255, take each IP as a C-Class address
    if (iIPAddr >= 167772160 and iIPAddr <= 184549375) or (iIPAddr >= 2886729728 and iIPAddr <= 2887778303) or (iIPAddr >= 3232235520 and iIPAddr <= 3232301055 ):
        netAddr = IP(iIPAddr & 0xFFFFFF00)
        return netAddr.strNormal()
    else:
        return 'outer_net'

####### judge whether a IP Address is internal network address or not #######
def isInternalNetwork(iIPAddr):
    #for the reserved IP address, eg: 10.0.0.0-10.255.255.255; 172.16.0.0-172.31.255.255; 192.168.0.0-192.168.255.255, take them as internal network
    if (iIPAddr >= 167772160 and iIPAddr <= 184549375) or (iIPAddr >= 2886729728 and iIPAddr <= 2887778303) or (iIPAddr >= 3232235520 and iIPAddr <= 3232301055 ):
        return True
    else:
        return False

####### the class definition of CPortScan #######
class CPortScan:
    def connectDB(self, strDBAddr, iDBPortNumber):
        kerberosAuthenticate()
        self.conn = connect(host = strDBAddr, port = iDBPortNumber, auth_mechanism='GSSAPI')
        self.cursor = self.conn.cursor()
        print "DB connect success.\n"
    
    
    def closeDB(self):
        self.cursor.close()
        self.conn.close()
        print "DB close success.\n"


    def fetchOneDay(self,outfile):
        listdict={}
        strSQL = "select sip,count(distinct(dip)) from armindb.parquet_armin_flow where year='2014' and month ='10' and day='13' group by sip"
        self.cursor.execute(strSQL)
        datalist=[]
        for flow in self.cursor:
            rowdict={}
            rowdict["sip"]=IP(flow[0]).strNormal()
            rowdict["dipnum"]=flow[1]
            datalist.append(rowdict)
        strSQL = "select sip,count(distinct(dpt)) from armindb.parquet_armin_flow where year='2014' and month ='10' and day='13' group by sip"
        self.cursor.execute(strSQL)
        for flow in self.cursor:
            for item in datalist:
                if item["sip"] == IP(flow[0]).strNormal():
                    if item["dipnum"]>=5 or flow[1]>=5:
                        item["dptnum"]=flow[1]
                    else:
                        datalist.remove(item)
                    break
        listdict["nodes"]=datalist
        jsonFile = file(outfile,'wb')
        encodedjson = json.dumps(listdict)
        jsonFile.write(encodedjson)

    def fetchdata(self,outfile):
        alldict={}
        for x in range(7):
            y = x + 13
            strSQL = "select hour,sip,count(distinct(dip)) from armindb.parquet_armin_flow where year='2014' and month ='10' and day='"+str(y)+"' group by hour,sip"
            self.cursor.execute(strSQL)
            for flow in self.cursor:
                ip = IP(flow[1]).strNormal()
                if not alldict.has_key(ip):
                    alldict[ip]=[]
                rowdict={}
                rowdict["day"]=x
                rowdict["hour"]=flow[0]
                rowdict["dipnum"]=flow[2]
                alldict[ip].append(rowdict)
            sql = "select hour,sip,count(distinct(dpt)) from armindb.parquet_armin_flow where year='2014' and month ='10' and day='"+str(y)+"' group by hour,sip"
            self.cursor.execute(sql)
            for flow in self.cursor:
                i = IP(flow[1]).strNormal()
                for item in alldict[i]:
                    if item["hour"] == flow[0] and item["day"]==x:
                        item["dptnum"]=flow[2]
                        a=item["dipnum"]/item["dptnum"]
                        b=item["dptnum"]/item["dipnum"]
                        if a>=b:
                            item["value"]=a
                        else:
                            item["value"]=b
                        break
        jsonFile = file(outfile,'wb')
        encodedjson = json.dumps(alldict)
        jsonFile.write(encodedjson)
    def fetchbyip(self,outfile,ip):
        alldict={}
        alldict[ip]=[]
        for x in range(7):
            print x
            y = x + 13
            strSQL = "select hour,count(distinct(dip)) from armindb.parquet_armin_flow where sip="+str(ip)+" and year='2014' and month ='10' and day='"+str(y)+"' group by hour"
            self.cursor.execute(strSQL)
            for flow in self.cursor:
                rowdict={}
                rowdict["day"]=x
                rowdict["hour"]=flow[0]
                rowdict["dipnum"]=flow[1]
                alldict[ip].append(rowdict)
            sql = "select hour,count(distinct(dpt)) from armindb.parquet_armin_flow where sip="+str(ip)+" and year='2014' and month ='10' and day='"+str(y)+"' group by hour"
            self.cursor.execute(sql)
            for flow in self.cursor:
                for item in alldict[ip]:
                    if item["hour"] == flow[0] and item["day"]==x:
                        item["dptnum"]=flow[1]
                        a=item["dipnum"]/item["dptnum"]
                        b=item["dptnum"]/item["dipnum"]
                        if a>=b:
                            item["value"]=a
                        else:
                            item["value"]=b
                        break
        jsonFile = file(outfile,'wb')
        encodedjson = json.dumps(alldict)
        jsonFile.write(encodedjson)
    
if __name__ =='__main__':     

    strDBAddr = '192.168.55.246'
    iDBPortNumber = 21050
    PortScan = CPortScan()
    PortScan.connectDB(strDBAddr,iDBPortNumber)
    PortScan.fetchdata("allScan.json")
    PortScan.closeDB()
