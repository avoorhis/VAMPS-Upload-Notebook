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
from pyfasta import Fasta

class RDP_File_Parser(object):
    
    def __init__(self, rdpFile=None, uniquesFile=None, namesFile=None, outFile=None, min_boot=0.8 ):
        self.rdpFile = rdpFile
        self.uniquesFile = uniquesFile
        self.namesFile = namesFile
        self.outFile = outFile
        self.min_boot = min_boot        
        self.tax_collector={}        # tax_collector[seqid] = 'Bacteria;Proteobacteria'
        self.seq_collector = {}      # seq_collector[seqid] = sequence -- from uniqs file file
        self.seq_info = {}           # seq_info[seqid]      = boot,rank -- from names file
        
    def parse_file(self):   
        #f1 = open(rdpFile,'r')
        with open(self.rdpFile) as f1:            
            for line in f1: 
                
                items = line.strip().split('\t')
                
                seqid = items[0].split('|')[0]
                self.seq_info[seqid] = {}
                self.tax_collector[seqid] = 'Unknown'
                #['FX18VMI01EPTIR', 'Bacteria', 'domain', '0.98', 'Cyanobacteria/Chloroplast', 'phylum', '0.53', 'Cyanobacteria', 'class', '0.5', 'Family', 'I', 'order', '0.49', 'Family', 'I', 'family', '0.49', 'GpI', 'genus', '0.49']
                for i,item in enumerate(items):
                    if item == 'domain':
                        this_boot = float(items[i+1].strip("'"))
                        if this_boot >= self.min_boot:
                            #dboot = float(items[i+1].strip("'"))
                            self.tax_collector[seqid] = items[i-1].strip('"').strip("'")
                            self.seq_info[seqid]["rank"] = 'domain'
                            self.seq_info[seqid]["boot"] = this_boot
                        else:                                                       
                            break  
                    
                    if item == 'phylum':
                        this_boot = float(items[i+1].strip("'"))
                        if this_boot >= self.min_boot:
                            self.tax_collector[seqid] += ';'+items[i-1].strip('"').strip("'")
                            self.seq_info[seqid]["rank"] = 'phylum' 
                            self.seq_info[seqid]["boot"]  = this_boot                           
                        else:                           
                            break
                    if item == 'class':
                        this_boot = float(items[i+1].strip("'"))
                        if this_boot >= self.min_boot:
                            self.tax_collector[seqid] += ';'+items[i-1].strip('"').strip("'")
                            self.seq_info[seqid]["rank"] = 'class'
                            self.seq_info[seqid]["boot"]  = this_boot
                        else:                            
                            break
                    if item == 'order':
                        this_boot = float(items[i+1].strip("'"))
                        if this_boot >= self.min_boot:
                            self.tax_collector[seqid] += ';'+items[i-1].strip('"').strip("'")
                            self.seq_info[seqid]["rank"] = 'order'
                            self.seq_info[seqid]["boot"]  = this_boot
                        else:                            
                            break        
                    if item == 'family':
                        this_boot = float(items[i+1].strip("'"))
                        if this_boot >= self.min_boot:
                            self.tax_collector[seqid] += ';'+items[i-1].strip('"').strip("'")
                            self.seq_info[seqid]["rank"] = 'family'
                            self.seq_info[seqid]["boot"]   = this_boot
                        else:                            
                            break           
                    if item == 'genus':
                        this_boot = float(items[i+1].strip("'"))
                        if  this_boot >= self.min_boot:
                            self.tax_collector[seqid] += ';'+items[i-1].strip('"').strip("'")
                            self.seq_info[seqid]["rank"] = 'genus'
                            self.seq_info[seqid]["boot"]  = this_boot
                        else:                            
                            break           
        f1.close()        
        f2 = Fasta(self.uniquesFile)
        for seqid in f2:
            self.seq_collector[seqid] = f2[seqid][:]         
                 
                
        with open(self.namesFile) as f3:           
            for line in f3:
                items = line.strip().split()
                seqid = items[0]
                copies = items[1].split(',')[1:]  # discard the first                
                self.seq_info[seqid]["count"] = 1
                for n in copies:
                    self.seq_info[seqid]["count"] += 1
        
    def write_file(self):
        
        f4 = open(self.outFile,'w')
        for seqid in self.seq_collector:
            # seqid sequence, taxonomy, boot, rank count
            #print(   '\t'.join([seqid, self.seq_collector[seqid],   self.tax_collector[seqid],   str(self.seq_info[seqid]["boot"]),  self.seq_info[seqid]["rank"],str(self.seq_info[seqid]["count"]) ]))
            f4.write("\t".join(
            [seqid, self.seq_collector[seqid],
            self.tax_collector[seqid], 
            str(self.seq_info[seqid]["boot"]), 
            self.seq_info[seqid]["rank"],
            str(self.seq_info[seqid]["count"]) ]))
            f4.write("\n")
        f4.close()
        
        
                        
                
                
if __name__ == '__main__':
    inst = RDP_File_Parser(rdpFile='work/ds1/rdpfile', uniquesFile='work/ds1/uniques.fa', namesFile='work/ds1/names', outFile='work/ds1/out', min_boot=0.8)
    inst.parse_file()
    inst.write_file()