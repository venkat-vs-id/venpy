#!/usr/bin/python -tt 

__version__ = '1.0'

import sys
import time
import re
import os
import subprocess
import datetime as dt
import json
import csv
from os import listdir
from  venpy.logger 	import MyLogger
from  venpy.xml2rd 	import XML2RD

def test_xml2rd():
	logger = MyLogger(pTag=0)
	xml2rd	= XML2RD()
	
	#-------- some advanced options   ---------------
	#If you want to force a specific XML Level into a separate table.
	xml2rd.arr_predefined_xmlPath4Tables = ['/items/item/topping']
	
	#If you would to add a prefix to all the tables created.
	xml2rd.tablename_prefix   = "mytab_"
	
	#If you would to add a prefix to all the CSVs created. ( like PROCESS_DATE etc)
	xml2rd.csv_prefix	= "ninjaCSV_"
	
	# to put multiple XMLPath into the same table. But make sure all XMLPATH are at the same level
	xml2rd.d_common_table_4XmlPaths = { '/items/item/batters':{'table':'common_table_batter', 'col_prefix':'b_', 'row_type':'batter'},
									  '/items/item/batterscost':{'table':'common_table_batter', 'col_prefix':'bc_', 'row_type':'batter_cost'}
									}
	
	#---- lets process a simple XML
	logger.log( "----- test for simple XML into CSV -----")
	l_pwd = os.path.dirname(os.path.abspath(sys.argv[0]))
	l_xml_file = os.path.join(l_pwd,'plant_catalog.xml')
	
	#---- lets process a NESTED XML
	logger.log( " ----- test for nested XML into CSV -----")
	l_pwd = os.path.dirname(os.path.abspath(sys.argv[0]))
	l_xml_file = os.path.join(l_pwd,'nested_adv.xml')
	
	#---- It loops the same xml 2times. firsttime to get the column definition and secondtime to write the CSV
	#--   If you just need CSV then you can sk
	for ix in range(1,3): 
		if ix == 1:
			#--get the table definition and i use this to dynamically create the tables in my DB
			ld_table_defn = xml2rd.process_xml_getDefn( pXML_pathfilename = l_xml_file, 
														pAddInfo		  = "info here goes into _add_info in the csv. I used this for zip file name") #--additional file info 				
			logger.log("--------------- Table Definition ----------------")
			logger.log(ld_table_defn)
			logger.log("-------------------------------------------------")
		elif ix == 2:
			#----Now process the files and generate CSV
			xml2rd.process_xml_csv(	pXML_pathfilename	= l_xml_file, 
									pAddInfo			= "info here goes into _add_info in the csv. I used this for zip file name") #--additional file info 				
									
			logger.log( "you can see the files in CSV directory - csv_dir")
		#end if		
	#end for
	
#-----------------------------------------------------------------------------------------------------------------
if __name__ == "__main__":  #will be called from command line
	test_xml2rd()