'''XML2RD
This class converts any XML document into a relational data. The idea is to create at table for every XML level and link levels using _id columns.
for eg:

---------------
change-history
---------------
'1.0|19-oct-2015|vsubr|created'
'''
__version__ = '1.0'

import io
import os
import re
import sys
from datetime import datetime
import xml.etree.ElementTree as ET
import collections as coll
import unicodecsv as csv

#-------  foxtel/AWS App related  --------
from   venpy.logger 	import MyLogger

logger = MyLogger(pTag=1)
#-----------------------------------------------------------------------------------------------------------------	
def removeNamespaces(pElemTag):
	if pElemTag[0] == "{":
		uri, tag = pElemTag[1:].split("}")
		return tag
	else:
		return pElemTag
	#end if
#-----------------------------------------------------------------------------------------------------------------	
class XML2RD(object):
	"""XML2RD: Converts XML into Relational data.
	"""
	def __init__(self):
		self.xmlfiles_csvfile_name			= "the_xml_files.csv"
		self.detailsOf_xmlfile_csvfile_name	= "details_of_"+self.xmlfiles_csvfile_name
		self.arr_predefined_xmlPath4Tables	= [] #[xmlpath]
		self.d_common_table_4XmlPaths		= {} #{xmlpath:{'table':<tablename>,'col_prefix':<>,'row_type':<>}}
		self.d_common_key_values			= {}  #this will added to all tables
		self.tablename_prefix				= "table_prefix_" #set this from calling script
		self.csv_prefix						= "ninjaCSV_"
		self.csv_dir						= os.path.join(os.path.dirname(os.path.abspath(sys.argv[0])),'csv_dir') #thats the default change it
		self.d_tablecolDef					= {} #{tablename:orderedDict{col:<colsize>}}		
		self.replaceStr4_xid				= '0'
		
		#--- protected
		self._process_action				= "defn" #defn/csv
		self._xid							= 0
		self._idx							= 0
		self._d_xml2tables					= {} #{xmlpath:[table_name,<count>]}
		self._d_rows						= {} #{tablename:[{<data_dict>}]}
		self._maxdup_col_count				= 20
#-----------------------------------------------------------------------------------------------------------------		
	def _fun_getRowType_and_prefix(self, pXML_Path):
		l_row_type   = '-'
		l_col_prefix = ''
		if pXML_Path in self.d_common_table_4XmlPaths.keys():
			l_row_type   = self.d_common_table_4XmlPaths[pXML_Path]['row_type']
			l_col_prefix = self.d_common_table_4XmlPaths[pXML_Path]['col_prefix']
		#end if	
		return l_row_type, l_col_prefix
#-----------------------------------------------------------------------------------------------------------------			
	def _fun_getTableName(self, pXML_Path):
	
		#--get the table defn if exists else create one
		if pXML_Path not in self._d_xml2tables.keys():
			if pXML_Path in self.d_common_table_4XmlPaths.keys():
				self._d_xml2tables[pXML_Path]	= [None,None]
				l_tableName = self.tablename_prefix + self.d_common_table_4XmlPaths[pXML_Path]['table']
				self._d_xml2tables[pXML_Path][0] = l_tableName.lower()
				self._d_xml2tables[pXML_Path][1] = 1 #count of records			
			else:
				l_tableName = self.tablename_prefix + os.path.basename(pXML_Path)		
				self._d_xml2tables[pXML_Path]	= [None,None]
				self._d_xml2tables[pXML_Path][0]= l_tableName.lower()
				self._d_xml2tables[pXML_Path][1] = 1 #count of records
			#end if
		else:
			self._d_xml2tables[pXML_Path][1] += 1 #count of records
		#end if
		return self._d_xml2tables[pXML_Path][0]		
#-----------------------------------------------------------------------------------------------------------------
	def _fun_getTableColDefn(self, pTableName):
		
		#--see if table defn exists, if not create one
		if pTableName not in self.d_tablecolDef.keys():
			self.d_tablecolDef[pTableName] = coll.OrderedDict()
		#end if
		return self.d_tablecolDef[pTableName]  # this should be ordered dict ortherwise the CSV will not work
#-----------------------------------------------------------------------------------------------------------------
	def _fun_getTableRow4Data(self, pTableName):
		
		if pTableName not in self._d_rows.keys():
			self._d_rows[pTableName] = []
		#end if;
		ld_rows_data = {}
		self._d_rows[pTableName].append(ld_rows_data) # this adds the dict for the row data into the row array for that table
		return ld_rows_data #--this will return the dict for the row data
#-----------------------------------------------------------------------------------------------------------------
	def _fun_getID(self):
		
		self._idx += 1
		return self._idx
#-----------------------------------------------------------------------------------------------------------------
	def _setValues_or_Defn(self, pdData, pdDefn, pColName, pColValue):
		
		#-- we have to change all column name into lower as redshift changes everything into lower anyway
		l_newColName = pColName.lower()
		
		#--if the same colname exists then add "__"
		if l_newColName not in pdData.keys():
			pdData[l_newColName] = ""
		else: # else if the column already exists for this row
			l_found = False
			for ix in range(1,(self._maxdup_col_count+1)): # if there is more column than {self._maxdup_col_count} duplicates then we cant handle and will error.
				if (l_newColName+'__'+str(ix)) not in pdData.keys():
					l_newColName = l_newColName+'__'+str(ix)
					pdData[l_newColName] = ""
					l_found = True
					break;
				#end if
			#end for
			if not l_found:
				logger.exception("There are more than %s duplciates and we can't handle it" %(str(self._maxdup_col_count)))
			#end if
		#end if
		
		#--clean the column values here 
		l_col_Value = pColValue
		if pColValue:
			try:
				if type(l_col_Value) is str:
					l_col_Value = unicode(l_col_Value, errors= 'replace')
					l_col_Value = str(pColValue).replace('\n','')
				#end if
			except Exception, e:
				logger.exception( "===%s====%s ====%s" % (str(e), pColValue, type(pColValue) ) )
			#end try
		#end if
		
		if self._process_action == "csv":  # then populate the data
			#populate the data
			pdData[l_newColName] = l_col_Value
		else:
			if re.match('^\_', l_newColName) and l_newColName not in ('_xid', '_xpath', '_rowtype') \
			   and l_newColName not in self.d_common_key_values.keys() :  # they are the id, parent_id and superParent_ID colums.. dont need size for them
			   
				pdDefn[l_newColName] = -1 # because they are all bigint
			else:
				if l_newColName not in pdDefn.keys():
					pdDefn[l_newColName] = 0
				#end if
				l_prv_len = pdDefn[l_newColName]
				
				l_len  = len(l_col_Value) if l_col_Value is not None else 0
				
				#--if the current len is greater than the previous len, then update it.
				if l_len > l_prv_len:
					pdDefn[l_newColName] = l_len
				#end if
		#end if
#-----------------------------------------------------------------------------------------------------------------	
	def _r_process_elem_data(self, pRecurLvl, pElement, pdParent, pdSuperParent):
		"""this is a recursive proc
		"""
		#--root_path is equal to a table
		l_elem_tag = removeNamespaces(pElement.tag)
		l_parent_path = pdParent['path']
		l_root_path   = l_parent_path+'/' + l_elem_tag
		
		#--get tableName and This will increment the rowCount
		l_tablename = self._fun_getTableName(l_root_path)
		
		#--get row_type if it is there( it will be there if we are using xml2rd.d_common_table_4XmlPaths )
		l_row_type, l_col_prefix = self._fun_getRowType_and_prefix(l_root_path)
		
		
		#--table Ids and its column name
		l_id 				= self._fun_getID()
		l_id_col			= l_tablename+'_id'
		l_parentID  		= pdParent['id']
		l_parentID_col  	= pdParent['id_col']
		l_super_parentID	= pdSuperParent['id']
		l_super_parentID_col= pdSuperParent['id_col']

		#--make the pdparent and pdSuperParent for the childs
		ld_parent 		= {'path':l_root_path,'id':l_id,'id_col':l_id_col}
		ld_super_parent = pdParent
		
		#--print to see if everything is correct
		logger.log("%s | Root=%s | current = %s " % (str(pRecurLvl), l_root_path, ld_parent))			
		logger.log("%s | Root=%s | Parent  = %s " % (str(pRecurLvl), l_root_path, pdParent))			
		logger.log("%s | Root=%s | super   = %s " % (str(pRecurLvl), l_root_path, pdSuperParent))			
		
		#--get the pointer to the tableColDefn
		ld_table_col_defn = self._fun_getTableColDefn(l_tablename) 
		
		#--get the pointer to the row data
		ld_row_data	= self._fun_getTableRow4Data(l_tablename)
		
		#-- add the standard columns to the table_defn. underscore(_) is added so that the column names will not clash with the xml elements( just in case)
		#-- if the defn is already there the code will only add the values. [$$dict rocks..$$] "-1" because they are all bigints so we dont need the size for them
		self._setValues_or_Defn( ld_row_data, ld_table_col_defn, '_'+l_id_col, l_id)
		self._setValues_or_Defn( ld_row_data, ld_table_col_defn, '_'+l_parentID_col, l_parentID)
		self._setValues_or_Defn( ld_row_data, ld_table_col_defn, '_'+l_super_parentID_col, l_super_parentID)
		self._setValues_or_Defn( ld_row_data, ld_table_col_defn, '_xid', self._xid)
		self._setValues_or_Defn( ld_row_data, ld_table_col_defn, '_xpath', l_root_path)

		#--add common values
		for cx in self.d_common_key_values.keys():
			self._setValues_or_Defn( ld_row_data, ld_table_col_defn, cx, self.d_common_key_values[cx])  #--value/text of the pElement
		#end if
		self._setValues_or_Defn( ld_row_data, ld_table_col_defn, l_col_prefix+l_elem_tag, pElement.text)  #--value/text of the pElement
		
		#logger.log( "%s --- %s" % (pElement.tag,  ET.tostring(pElement, encoding='utf8', method='xml') ), pTag=1)
		#--load attributes if the element has attributes
		if len(pElement.attrib.keys())>0:
			for ax in pElement.attrib.keys():
				l_col_attrib_name = l_elem_tag+'#'+removeNamespaces(ax)  # to remove the stupid namespaces which creates the problem
				l_col_attrib_val  =	pElement.attrib[ax]
				self._setValues_or_Defn( ld_row_data, ld_table_col_defn, l_col_prefix+l_col_attrib_name, l_col_attrib_val)
			#end for
		#end if
		
		#-- get the element in each root_path ie columns for the each tables
		for dx in list(pElement):	
			
			l_dx_tag  = removeNamespaces(dx.tag)
			l_dx_path = l_root_path+'/'+l_dx_tag
			
			#-- if the current element has childs 
			#-- OR current element has no childs but our previous processing shows that it had childs 
			#-- OR if the developer explicitly says put this element as a separate table
			#-- then treat it as a parent and process that node separately
			if len(list(dx))>0  or ( l_dx_path in self._d_xml2tables.keys()) or ( l_dx_path in self.arr_predefined_xmlPath4Tables):
				self._r_process_elem_data( pRecurLvl+1, dx, ld_parent, ld_super_parent)
			else:
				#----This is the area where the columns for each row is retrived.
				#--  load attributes if the element has attributes
				if len(dx.attrib.keys())>0:
					for ax in dx.attrib.keys():
						l_col_attrib_name = l_dx_tag+'#'+removeNamespaces(ax)  # to remove the stupid namespaces which creates the problem
						l_col_attrib_val  =	dx.attrib[ax]
						self._setValues_or_Defn( ld_row_data, ld_table_col_defn, l_col_prefix+l_col_attrib_name, l_col_attrib_val)
					#end for
				#end if

				#--- it comes here only when there are elements. [$$ atleast thats wat i think :) $$]
				self._setValues_or_Defn( ld_row_data, ld_table_col_defn, l_col_prefix+l_dx_tag, dx.text)
			#end if
		#end for		
#-----------------------------------------------------------------------------------------------------------------		
	def _write2csv(self):
		for eachTable in self._d_rows.keys():  #self._d_rows = #{tablename:[{<data_dict>}]}
			
			if not os.path.exists(self.csv_dir):
				os.makedirs(self.csv_dir)				
			#end if			
			
			#get teh column order from tableColDef
			larr_colsInOrder  = self.d_tablecolDef[eachTable].keys()  #{tablename:orderedDict{col:<colsize>}}
			l_csvFilename	  =  eachTable + '.csv'
			
			#--for these files(the_xml_files.csv & details_of_the_xml_files.csv) dont add teh prefix  
			if l_csvFilename not in ( self.xmlfiles_csvfile_name, self.detailsOf_xmlfile_csvfile_name):
				l_csvFilename     = self.csv_prefix +l_csvFilename
			#end if
			l_csvPathFilename = os.path.join(self.csv_dir,l_csvFilename)
			
			#--get all the rows
			larr_rows_data	  = self._d_rows[eachTable] #[{dict of row}, {dict of 2nd row}]
			
			#--check if the file was created in this instance, if not create a new one or overwite the old file (if already exists)
			l_add_HeaderLine = True
			l_file_open_mode = 'wb'
			
			# do not overwite any file if exists 
			if os.path.exists(l_csvPathFilename):
				l_add_HeaderLine = False
				l_file_open_mode = 'ab'
			#end if
			
			#open csv
			with open(l_csvPathFilename, l_file_open_mode) as f:
				
				#-- handle to the csv writer which set the order of the columns
				csvWritter = csv.DictWriter(f, larr_colsInOrder)
				
				#-- for the first time add the header line
				if l_add_HeaderLine:
					csvWritter.writeheader()
				#end if
				
				for dRowx in larr_rows_data:
					try:
						#ld_tmp_rowx = {k:unicode(v, errors="ignore") if type(v) is str else v for k,v in dRowx.items()}
						csvWritter.writerow(dRowx)
						#csvWritter.writerow(ld_tmp_rowx)
					except Exception, e:
						logger.exception( "Error while converting into CSV = %s row=%s" % (str(e), ld_tmp_rowx) )
					#end try
					ld_tmp_rowx = None
				#end for
			#end with
		#end for
#-----------------------------------------------------------------------------------------------------------------			
	def _process_xml_main(self, pXML_pathfilename, pAddInfo):
		"""		
		"""
		logger.info("------start of file = %s - action=%s" % (pXML_pathfilename,self._process_action))
		
		#reset some variables(if any)
		self._xid	= 0
		self._idx	= 0		
		
		# id for the xmlfile
		l_xmlfilename 	= os.path.basename(pXML_pathfilename)
		self._xid 		= re.sub(self.replaceStr4_xid,'',pXML_pathfilename)

		#--now process the xml files
		l_path 	  		= ""
		l_parent_path 	= ""
		l_idx	  		= 0
		l_parent_level	= -1
		
		#--loop through each element and process it
		for event, elem in ET.iterparse(pXML_pathfilename, events=('start', 'end')):#'start-ns', 'end-ns' cant handle namespace		
			l_idx +=1		
			l_elem_tag	= removeNamespaces(elem.tag)  # this removes the namespaces
			logger.log( "elem----- %s" % l_elem_tag, pTag=0)			

			#----- when each ELEMENT starts ------------
			if event in ('start', 'start-ns'):
				l_path = l_path+'/' + l_elem_tag
				
				#This means a newlevel is starting
				if l_parent_path != os.path.dirname(l_path):
					l_parent_level += 1
					l_parent_path 	= os.path.dirname(l_path)
				#end if
			#end if;

			#----- when each ELEMENT ends ------------
			if event in ('end', 'end-ns'):
				#-- remove the element from the path
				l_path = re.sub('/'+l_elem_tag+'$','',l_path)			

				#This mean the level is endling
				if l_parent_path != l_path:
					l_parent_level -= 1
					l_parent_path 	= l_path
				#end if

				#--if the parent_level = 1 then thats one complete MASTER record. now process it.
				if l_parent_level == 1:				
					ld_parent 		= {'path':l_parent_path,'id':0,'id_col':'PNA'}
					ld_Superparent 	= {'path':"/",'id':0,'id_col':'SNA'}
					self._r_process_elem_data(0, elem, ld_parent, ld_Superparent)
					
					#push the data into file
					if self._process_action == 'csv':
						logger.log("put the data into the file", pTag=0)
						self._write2csv()
					#end if
					
					#---- clear some memory - GC
					self._d_rows = None # for GC
					self._d_rows = {}
					#--clear the element here so that the memory is not clogged
					elem.clear()

					#logger.log( "END of = %s ~ parent=%s  ~ level:%s " %( elem.tag, l_parent_path, str(l_parent_level) ), pTag=1)					
				#end if
			#end if;
			#if l_idx == 100000050: break;
		#end for //end the XML
		
		#----------------------------------------------
		#--- rows for detailsof_theXMLFiles.csv ------
		#----------------------------------------------
		l_tablename = self.detailsOf_xmlfile_csvfile_name.replace('.csv','')	
		#--get the pointer to the tableColDefn
		ld_table_col_defn = self._fun_getTableColDefn(l_tablename) 
		
		#get each table and its rowcount
		for ex in self._d_xml2tables.keys():
			#--get the pointer to the row data
			ld_row_data	= self._fun_getTableRow4Data(l_tablename)
			ld_colDef	= self.d_tablecolDef[l_tablename]
			l_colDef_str = ','.join(ld_colDef.keys())
			
			self._setValues_or_Defn( ld_row_data, ld_table_col_defn, '_xid', self._xid)
			self._setValues_or_Defn( ld_row_data, ld_table_col_defn, '_xpath', ex)
			self._setValues_or_Defn( ld_row_data, ld_table_col_defn, 'table_name', self._d_xml2tables[ex][0])
			self._setValues_or_Defn( ld_row_data, ld_table_col_defn, 'cols', l_colDef_str)
			self._setValues_or_Defn( ld_row_data, ld_table_col_defn, 'rcd_count', self._d_xml2tables[ex][0])
			for cx in self.d_common_key_values.keys():
				self._setValues_or_Defn( ld_row_data, ld_table_col_defn, cx, self.d_common_key_values[cx])  #--value/text of the pElement
			#end if			
		#end for

		#----------------------------------------
		#--- rows for theXMLFiles.csv ------
		#----------------------------------------
		l_tablename = self.xmlfiles_csvfile_name.replace('.csv','')	
		#--get the pointer to the tableColDefn
		ld_table_col_defn = self._fun_getTableColDefn(l_tablename) 
		#--get the pointer to the row data
		ld_row_data	= self._fun_getTableRow4Data(l_tablename)
		self._setValues_or_Defn( ld_row_data, ld_table_col_defn, '_xid', self._xid)
		self._setValues_or_Defn( ld_row_data, ld_table_col_defn, '_add_info', pAddInfo)
		for cx in self.d_common_key_values.keys():
			self._setValues_or_Defn( ld_row_data, ld_table_col_defn, cx, self.d_common_key_values[cx])  #--value/text of the pElement
		#end if					
		
		#--write to csv for thexmlfiles.csv and details of details_of_xmlfiles
		if self._process_action == 'csv':
			self._write2csv()
		#end if
		#---- clear some memory - GC
		self._d_rows = None # for GC
		self._d_rows ={}
					
		logger.log("========= ALL done =================")
		if self._process_action == "defn":
			logger.log(self._d_xml2tables,pTag=1)
			logger.log(self.d_tablecolDef,pTag=1)
		elif self._process_action == "csv":
			pass
		#end if
		logger.info("******* end of file = %s" % pXML_pathfilename)
		logger.log("========= ALL done =================")
		
		return self.d_tablecolDef
#-----------------------------------------------------------------------------------------------------------------				
	def process_xml_csv(self, pXML_pathfilename, pAddInfo):
		
		logger.log("l_csvDir=====%s" % self.csv_dir)
		
		self._process_action 	= 'csv'
		res = self._process_xml_main( pXML_pathfilename = pXML_pathfilename, 
									  pAddInfo 			= pAddInfo)
		return 0
#-----------------------------------------------------------------------------------------------------------------			
	def process_xml_getDefn(self, pXML_pathfilename, pAddInfo):		
		
		logger.log("l_csvDir=====%s" % self.csv_dir)
		
		self._process_action 	= 'defn'
		ld_defn = self._process_xml_main( pXML_pathfilename = pXML_pathfilename, 
										  pAddInfo 			= pAddInfo
										 )
		return ld_defn
#-----------------------------------------------------------------------------------------------------------------	
