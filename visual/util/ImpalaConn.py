from impala.dbapi import connect


def connectDB(self, strDBAddr, iDBPortNumber):
        self.conn = connect(host = strDBAddr, port = iDBPortNumber, auth_mechanism='GSSAPI')
        self.cursor = self.conn.cursor()
        print "DB connect success.\n"
def closeDB(self):
        self.cursor.close()
        self.conn.close()
        print "DB close success.\n"