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
import venpy.logger as l

def test_logger():
	logger = MyLogger(pTag=0)
	
	#normal Log
	logger.log( "---------------------------------------------------------")
	logger.log( "hello world")
	logger.info( "Info Message")
	logger.warning( "Warning Message")
	logger.log( "---------------------------------------------------------")
	
	#if you want to see the linenumber in all debug messages.. set the LOG_SHOW_LINE_NO = True in logger.py
	logger.show_line_no()
	logger.log( "hello world with line number")
	logger.remove_show_line_no()
	logger.log( "---------------------------------------------------------")

	#exceptions will stop and exit the code. will show the line number
	try:
		a = 1/0
	except:
		logger.exception( "Raise Exception and will also show line number")
	#end try
	logger = None	
#-----------------------------------------------------------------------------------------------------------------
if __name__ == "__main__":  #will be called from command line
	test_logger()