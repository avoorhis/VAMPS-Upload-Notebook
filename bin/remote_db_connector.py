#!/usr/bin/env python3
# -*- coding: utf-8 -*-

############################################################
#                                                          #
# Simple script to connect to a remote mysql database      #
#                                                          #
#                                                          #
# Install MySQLdb package by running:                      #
#                                                          #
#                       pip install MySQL-python           #
#                                                          #
############################################################

class MySQL_Connect(object):

    def __init__(self, HOST='localhost', DB='vamps_development', USER='ruby', PWORD='ruby'):
        import pymysql as db

        PORT = 3306
        
        self.conn = db.Connection(host=HOST, port=PORT,
                                   user=USER, passwd=PWORD, db=DB)
        self.cur = self.conn.cursor()
        
    def print_test_list(self):
        self.cur.execute("SELECT * from user")
        result = self.cur.fetchall()
        for item in result:
            print(item)



if __name__ == '__main__':
    inst = MySQL_Connect()
    inst.print_test_list()