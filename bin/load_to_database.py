#!/usr/bin/env python3

##!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) 2011, Marine Biological Laboratory
#
# This program is free software; you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free
# Software Foundation; either version 2 of the License, or (at your option)
# any later version.
#
# Please read the COPYING file.
#

import os
from stat import * # ST_SIZE etc
import sys
import shutil
import types
import time
import random
import csv
from time import sleep
#import ConfigParser
import datetime
import logging
import subprocess
import unicodedata
import pprint

today = str(datetime.date.today())
pp = pprint.PrettyPrinter(indent=4)

ranks =['domain','phylum','klass','order','family','genus','species','strain']

classifiers = {"GAST":{'ITS1':1,'SILVA108_FULL_LENGTH':2,'GG_FEB2011':3,'GG_MAY2013':4},
                "RDP":{'ITS1':6,'2.10.1':5,'GG_FEB2011':7,'GG_MAY2013':8},
                'unknown':{'unknown':9}}
sys.path.append('/Users/avoorhis/programming/jupyter/bin')
def mysql_connection(host, db, user, passw):
    
    from remote_db_connector import MySQL_Connect
    sqlcur = MySQL_Connect(HOST=host, DB=db, USER=user, PWORD=passw) 
       #              read_default_file="~/.my.cnf_node"  )
    return sqlcur
    
            
class Load_CSV_Data_File(object):

    def __init__(self, inFile=None, project='default', dataset='ds1', user='admin', public=1, dbhost='localhost', dbname=None, dbuser=None, dbpass=None ):
        
        self.RANK_COLLECTOR = {}
        self.SEQ_COLLECTOR = {}
        self.DATASET_ID_BY_NAME = {}
        self.SUMMED_TAX_COLLECTOR = {}
        self.TAX_ID_BY_RANKID_N_TAX = {}
        self.SILVA_IDS_BY_TAX = {}
        self.CONFIG_ITEMS = {}
        self.infile = inFile
        self.CONFIG_ITEMS['project']    = project
        self.CONFIG_ITEMS['dataset']    = dataset
        self.CONFIG_ITEMS['user']       = user
        self.CONFIG_ITEMS['public']     = public
        
        if dbname or dbuser or dbpass:
            self.db = mysql_connection(dbhost, dbname, dbuser, dbpass)
        elif dbhost == 'localhost':
            self.db = mysql_connection(dbhost, 'vamps_development', 'ruby', 'ruby')
        elif dbhost == 'vampsdev':
            self.db = mysql_connection(dbhost, 'vamps2', 'vamps_w', 'g1r55nck')
        else:
            sys.exit('No MySQL connection - Exiting')
        
        self.check_user()               # script dies if user not in db
        self.check_project()            # Check if project already exists
        self.recreate_ranks()
        self.push_taxonomy()
        self.push_sequences()
        self.push_project()
        self.push_dataset()
        self.push_pdr_seqs()
        
        print(self.RANK_COLLECTOR)
        print(self.CONFIG_ITEMS)
        
    def check_user(self):
            """
            check_user()
            the owner/user must be present in 'user' table for script to continue
            """
            q = "select user_id from user where username='"+self.CONFIG_ITEMS['user']+"'"
            print(q)
            self.db.cur.execute(q)
            numrows = int(self.db.cur.rowcount)
            if numrows==0:
                sys.exit('Could not find owner: '+self.CONFIG_ITEMS['user']+' --Exiting')
            else:
                row = self.db.cur.fetchone()
                self.CONFIG_ITEMS['owner_id'] = row[0]
                
    def check_project(self):
        """
        check_project()
        the project must not already exist in db table 'project' for script to continue
        """
        q = "select project_id from project where project='"+self.CONFIG_ITEMS['project']+"'"
        print(q)
        self.db.cur.execute(q)
        numrows = int(self.db.cur.rowcount)
        if numrows > 0:
            sys.exit('Project already Exists: '+self.CONFIG_ITEMS['project']+' --Exiting')
            
    def recreate_ranks(self):
        for i,rank in enumerate(ranks):

            q = "INSERT IGNORE into rank (rank,rank_number) VALUES('"+rank+"','"+str(i)+"')"
            logging.debug(q)
            self.db.cur.execute(q)
            rank_id = self.db.cur.lastrowid
            if rank_id==0:
                q = "SELECT rank_id from rank where rank='"+rank+"'"
                logging.debug(q)
                self.db.cur.execute(q)
                row = self.db.cur.fetchone()
                self.RANK_COLLECTOR[rank] = row[0]
            else:
                self.RANK_COLLECTOR[rank] = rank_id
        q = "INSERT IGNORE into rank (rank,rank_number) VALUES('superkingdom','0'),('NA','0')"
        self.db.cur.execute(q)
        self.db.conn.commit()

    def push_taxonomy(self):

        #print  general_config_items
        silva = ['domain_id','phylum_id','klass_id','order_id','family_id','genus_id','species_id','strain_id']
        accepted_domains = ['bacteria','archaea','eukarya','fungi','organelle','unknown']
        tax_collector = {}


        print( 'csv '+self.infile)
        lines = csv.reader(open(self.infile,"r"), delimiter='\t')
        #print tax_file

        for line in lines:
            #items = line.split('\t')
            # FX18VMI01B8S4P	ACATCCAACGCGAAAAACCTTACCCGGACTAGAATGTGAGGGAATATATCAGAGATGGTATAGTCAGCAATGACCCGAAACAAGGTGATGCATGGTTGTC	Bacteria	0.98	domain	3
            print(line)            
            
            #print line
            seqid       = line[0]
            seq         = line[1]
            tax_string  = line[2]
            boot_score  = line[3]
            rank        = line[4]
            seq_count   = line[5]
                  

            if rank == 'class': rank = 'klass'
            if rank == 'orderx': rank = 'order'
            
            self.SEQ_COLLECTOR[seq] = {'dataset':self.CONFIG_ITEMS['dataset'],
                                  'taxonomy':tax_string,                                  
                                  'rank':rank,
                                  'seq_count':seq_count,
                                  'boot':boot_score
                                  }
            q1 = "SELECT rank_id from rank where rank = '"+rank+"'"
            print( q1)
            self.db.cur.execute(q1)
            self.db.conn.commit()

            row = self.db.cur.fetchone()

            self.SEQ_COLLECTOR[seq]['rank_id'] = row[0]

            tax_items = tax_string.split(';')
            #print tax_string
            sumtax = ''
            for i in range(0,8):

                rank_id = self.RANK_COLLECTOR[ranks[i]]
                if len(tax_items) > i:

                    taxitem = tax_items[i]

                else:
                    taxitem = ranks[i]+'_NA'
                sumtax += taxitem+';'

                #print ranks[i],rank_id,taxitem,sumtax,seq_count
                if rank_id in self.SUMMED_TAX_COLLECTOR:
                    if sumtax[:-1] in self.SUMMED_TAX_COLLECTOR[rank_id]:
                        self.SUMMED_TAX_COLLECTOR[rank_id][sumtax[:-1]] += int(seq_count)
                    else:
                        self.SUMMED_TAX_COLLECTOR[rank_id][sumtax[:-1]] = int(seq_count)

                else:
                    self.SUMMED_TAX_COLLECTOR[rank_id] = {}
                    self.SUMMED_TAX_COLLECTOR[rank_id][sumtax[:-1]] = int(seq_count)

            #for i in range(0,8):
            #insert_nas()

            if tax_items[0].lower() in accepted_domains:
                ids_by_rank = []
                for i in range(0,8):
                    #print i,len(tax_items),tax_items[i]
                    rank_name = ranks[i]
                    rank_id = self.RANK_COLLECTOR[ranks[i]]

                    if len(tax_items) > i:
                        if ranks[i] == 'species':
                            t = tax_items[i].lower()
                        else:
                            t = tax_items[i].capitalize()

                        if tax_items[i].lower() != (rank_name+'_NA').lower():
                            name_found = False
                            if rank_name in tax_collector:
                                tax_collector[rank_name].append(t)
                            else:
                                tax_collector[rank_name] = [t]
                    else:
                        t = rank_name+'_NA'



                    q2 = "INSERT ignore into `"+rank_name+"` (`"+rank_name+"`) VALUES('"+t+"')"
                    print(q2)
                    self.db.cur.execute(q2)
                    self.db.conn.commit()
                    tax_id = self.db.cur.lastrowid
                    if tax_id == 0:
                        q3 = "select "+rank_name+"_id from `"+rank_name+"` where `"+rank_name+"` = '"+t+"'"
                        logging.debug( q3 )
                        self.db.cur.execute(q3)
                        self.db.conn.commit()
                        row = self.db.cur.fetchone()
                        tax_id=row[0]
                    ids_by_rank.append(str(tax_id))
                    #else:
                    print( 'rank_id,t,tax_id '+str(rank_id)+' - '+t+' - '+str(tax_id)  )
                    if rank_id in self.TAX_ID_BY_RANKID_N_TAX:
                        self.TAX_ID_BY_RANKID_N_TAX[rank_id][t] = tax_id
                    else:
                        self.TAX_ID_BY_RANKID_N_TAX[rank_id]={}
                        self.TAX_ID_BY_RANKID_N_TAX[rank_id][t] = tax_id
                    #ids_by_rank.append('1')
                logging.debug(  ids_by_rank )
                q4 =  "INSERT ignore into silva_taxonomy ("+','.join(silva)+",created_at)"
                q4 += " VALUES("+','.join(ids_by_rank)+",CURRENT_TIMESTAMP())"
                #
                print(q4)
                self.db.cur.execute(q4)
                self.db.conn.commit()
                silva_tax_id = self.db.cur.lastrowid
                if silva_tax_id == 0:
                    q5 = "SELECT silva_taxonomy_id from silva_taxonomy where ("
                    vals = ''
                    for i in range(0,len(silva)):
                        vals += ' '+silva[i]+"="+ids_by_rank[i]+' and'
                    q5 = q5 + vals[0:-3] + ')'
                    print(q5)
                    self.db.cur.execute(q5)
                    self.db.conn.commit()
                    row = self.db.cur.fetchone()
                    silva_tax_id=row[0]

                self.SILVA_IDS_BY_TAX[tax_string] = silva_tax_id
                self.SEQ_COLLECTOR[seq]['silva_tax_id'] = silva_tax_id
                self.db.conn.commit()

        #print( 'SUMMED_TAX_COLLECTOR')
        #print( self.SUMMED_TAX_COLLECTOR)
        
    def push_sequences(self):
        # sequences
        
        for seq in self.SEQ_COLLECTOR:
            q = "INSERT ignore into sequence (sequence_comp) VALUES (COMPRESS('"+seq+"'))"
            print(q)
            self.db.cur.execute(q)
            self.db.conn.commit()
            seqid = self.db.cur.lastrowid
            if seqid == 0:
                q2 = "select sequence_id from sequence where sequence_comp = COMPRESS('"+seq+"')"
                print('DUP SEQ FOUND')
                self.db.cur.execute(q2)
                self.db.conn.commit()
                row = self.db.cur.fetchone()
                seqid=row[0]
            self.SEQ_COLLECTOR[seq]['sequence_id'] = seqid
            silva_tax_id = str(self.SEQ_COLLECTOR[seq]['silva_tax_id'])
            distance = str(self.SEQ_COLLECTOR[seq]['boot'])
            print( self.CONFIG_ITEMS['dataset']+' - '+seq+' - '+str(silva_tax_id))
            rank_id = str(self.SEQ_COLLECTOR[seq]['rank_id'])
            print( rank_id)
            q = "INSERT ignore into silva_taxonomy_info_per_seq"
            q += " (sequence_id,silva_taxonomy_id,gast_distance,refssu_id,rank_id)"
            q += " VALUES ('"+str(seqid)+"','"+silva_tax_id+"','"+distance+"','0','"+rank_id+"')"
            print(q)
            self.db.cur.execute(q)
            self.db.conn.commit()
            silva_tax_seq_id = self.db.cur.lastrowid
            print('1: '+str(silva_tax_seq_id))
            if silva_tax_seq_id == 0:
                q3 = "select silva_taxonomy_info_per_seq_id from silva_taxonomy_info_per_seq"
                q3 += " where sequence_id = '"+str(seqid)+"'"
                q3 += " and silva_taxonomy_id = '"+silva_tax_id+"'"
                q3 += " and gast_distance = '"+distance+"'"
                q3 += " and refssu_id = '0'"
                q3 += " and rank_id = '"+rank_id+"'"
                logging.debug('DUP silva_tax_seq')
                print(q3)
                self.db.cur.execute(q3)
                self.db.conn.commit()
                row = self.db.cur.fetchone()
                silva_tax_seq_id=row[0]
                print('0: '+str(silva_tax_seq_id))

            q4 = "INSERT ignore into sequence_uniq_info (sequence_id, silva_taxonomy_info_per_seq_id)"
            q4 += " VALUES('"+str(seqid)+"','"+str(silva_tax_seq_id)+"')"
            print(q4)
            self.db.cur.execute(q4)
            self.db.conn.commit()
        ## don't see that we need to save uniq_ids
        self.db.conn.commit()
        
    def push_project(self):
        desc = "Project Description"
        title = "Title"
        proj = self.CONFIG_ITEMS['project']
        rev = self.CONFIG_ITEMS['project'][::-1]
        fund = "myfunding"
        oid = self.CONFIG_ITEMS['owner_id']
        pub = self.CONFIG_ITEMS['public']
        fields = ['project','title','project_description','rev_project_name','funding','owner_user_id','public']
            
        
        # uncomment
        # q = "INSERT into project ("+(',').join(fields)+")"
        q = "INSERT IGNORE into project ("+(',').join(fields)+")"
        q += " VALUES('%s','%s','%s','%s','%s','%s','%s')"
        q = q % (proj,title,desc,rev,fund,oid,pub)
        print(q)
        self.db.cur.execute(q)
        self.db.conn.commit()
        self.CONFIG_ITEMS['project_id'] = self.db.cur.lastrowid
        print("NEW PID="+str(self.CONFIG_ITEMS['project_id']))
        print("STARTING NEW project -- PID="+str(self.CONFIG_ITEMS['project_id']))
        
    def push_dataset(self):
        print("IN push_dataset CONFIG_ITEMS")
        
        fields = ['dataset','dataset_description','project_id']
        q = "INSERT into dataset ("+(',').join(fields)+")"
        q += " VALUES('%s','%s','%s')"
        
        desc = self.CONFIG_ITEMS['dataset']+'_description'
        #print ds,desc,CONFIG_ITEMS['env_source_id'],CONFIG_ITEMS['project_id']
        q4 = q % (self.CONFIG_ITEMS['dataset'], desc,  self.CONFIG_ITEMS['project_id'])
        print(q4)
        self.db.cur.execute(q4)
        did = self.db.cur.lastrowid
        self.DATASET_ID_BY_NAME[self.CONFIG_ITEMS['dataset']] = str(did)
        print("DATASET_ID_BY_NAME")
        print(self.DATASET_ID_BY_NAME)
        self.db.conn.commit()
         
    def push_pdr_seqs(self):
    
        for seq in self.SEQ_COLLECTOR:
            did = self.DATASET_ID_BY_NAME[self.CONFIG_ITEMS['dataset']]
            seqid = self.SEQ_COLLECTOR[seq]['sequence_id']
            count = self.SEQ_COLLECTOR[seq]['seq_count']
            q = "INSERT into sequence_pdr_info (dataset_id, sequence_id, seq_count,classifier_id)"
            q += " VALUES ('"+str(did)+"','"+str(seqid)+"','"+str(count)+"','2')"
            print(q)
            self.db.cur.execute(q)
        self.db.conn.commit()


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="") 
    myusage = """
        -p/--project  project name          REQUIRED

        -s/--seqs_file       sequences file REQUIRED --FORMAT: see below
        -m/--metadata_file   metadata file  REQUIRED --FORMAT: see below

        -public/--public                    DEFAULT == '1'  true
        -env_source_id/--env_source_id      DEFAULT == '100' unknown
        -owner/--owner                      REQUIRED  (must be already in users table)
        -add/--add_project                  Will add to project

     
    """
    
    
    parser.add_argument("-p", "--project",    
                required=False,  action='store', dest = "project",  default='ptest',
                help="choices=['vampsdb','vampsdev','localhost']") 
                 
    if len(sys.argv[1:]) == 0:
        print(myusage)
        sys.exit() 
    args = parser.parse_args()
    
    

    #out_file = "tax_counts--"+NODE_DATABASE+".json"
    #in_file  = "../json/tax_counts--"+NODE_DATABASE+".json"

    inst = Load_CSV_Data_File(inFile='work/ds1/vamps_data.csv',project=args.project,dataset='ds1',user='admin',public=0,dbhost='localhost')

    

