'''Logger - This is custom wrapper for Python's built in Logging. We just use this to wrap the settings and to make it simple for the developers
---------------
change-history
---------------
'1.0|01-aug-2015|vsubr|created'
'''
__version__ = '1.0'
#------------------------------------------------------------------------------------------------------------------------
import sys
import os
import time
import logging
import logging.handlers
import inspect
import pprint
import collections
import traceback


ENV						= "DEV"
LOG_DEBUG_MODE			=	True
LOG_ON_FILE				= 	True	# log is written into a file
LOG_SHOW_LINE_NO		= 	False
LOG_FILE_DIR 			=	"D:\\logs\\venpy_logs" #directory where log files from py foxpy modules are written
LOG_FORMATTER 			= 	logging.Formatter('[%(asctime)s]-[%(levelname)-10s]>>>%(message)s')

#--------------------------------------------------------------------------------------------------------------------------------------------
class OnlyDebugMessages(logging.Filter):
	'''	Class for logging only debug messages
	'''
	def filter(self, record):
		return record.levelno == logging.DEBUG

#--------------------------------------------------------------------------------------------------------------------------------------------	
class MyLogger:
	'''	Class for logging
	'''
#--------------------------------------------------------------------------------------------------------------------------------------------	
	def __init__(self,pTag=0,pProcessName=None,pDebugMode=False):
		self._tag  			= pTag if pTag else 0
		self._debug_mode	= True if (pDebugMode or LOG_DEBUG_MODE) else False
		self._show_line_no	= True if (pDebugMode or LOG_SHOW_LINE_NO) else False
		self.__logger		= None;
		self._only_debug_obj= None
		
		l_processname	= pProcessName if pProcessName else "venpy_logger"
		self.__setup(l_processname)
		
#--------------------------------------------------------------------------------------------------------------------------------------------		
	def __del__(self):# like  a de-alloc function		
		pass
#--------------------------------------------------------------------------------------------------------------------------------------------	
	def __setup(self,pProcessName):
		
		# create logger with the processName
		self.__logger = logging.getLogger(pProcessName)
		
		# check if a logger already exists
		if (len(self.__logger.handlers) == 0):
			print("Logger.py: There is no logger for this process =%s" % (pProcessName) )
			#----create a logger	
			self.__logger.setLevel(logging.DEBUG)

			# create formatter and add it to the required handlers
			formatter = logging.Formatter('[%(asctime)s]-[%(levelname)-10s]=%(message)s')
			
			# create "file handler" which logs even debug messages
			if LOG_ON_FILE: # ie if it is true
				
				if not os.path.exists(LOG_FILE_DIR):
					os.makedirs(LOG_FILE_DIR)				
				#end if
				l_filename = time.strftime(pProcessName+"_%Y_%m_%d.log")
				rfh = logging.handlers.TimedRotatingFileHandler( os.path.join(LOG_FILE_DIR, l_filename), 
																when='MIDNIGHT', 
																#interval=1, 
																backupCount=10, # doesnt matter as we have dates on the filename.
																encoding=None, 
																delay=False, 
																utc=False)
				rfh.suffix = '_%Y%m%d.log'
				rfh.setLevel(logging.DEBUG)
				rfh.setFormatter(formatter)
				self.__logger.addHandler(rfh)
			#end if
			
			# create "console handler" with a higher log level
			ch = logging.StreamHandler()
			ch.setLevel(logging.DEBUG)
			ch.setFormatter(formatter)
			self.__logger.addHandler(ch)	
			
			self.__logger.info("Logger setup for process =%s is good", pProcessName)
		else:
			self.__logger.debug("logger Already exists for this process = %s" % pProcessName)	
		#end if
#---------------------------------------------------------------------------------------------------------------------------------------------------
	def warning(self, pMsg):
		self.__logger.warning(pMsg)
#---------------------------------------------------------------------------------------------------------------------------------------------------	
	def info(self, pMsg):
		self.__logger.info(pMsg)
#---------------------------------------------------------------------------------------------------------------------------------------------------
	def exception(self, pMsg, pExit=True):
		
		l_newMsg = pMsg
		frame 	= inspect.stack()[1]
		module 	= inspect.getmodule(frame[0])
		(l_frame, l_filename, l_line_number, l_function_name, l_lines, l_index) = inspect.getouterframes(inspect.currentframe())[1]
		l_newMsg = '%s {File:%s,Line:%s}' % (pMsg, l_filename, l_line_number)
		self.__logger.debug("~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ")
		self.__logger.error(l_newMsg)
		traceback.print_exc()
		self.__logger.debug("~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ")
		
		if pExit:
			sys.exit()
		#end if
#---------------------------------------------------------------------------------------------------------------------------------------------------
	def log(self, pMsg, pTag=0):
		
		if LOG_DEBUG_MODE and pTag >= self._tag:
			l_msg = pMsg
			
			if type(pMsg) is list or type(pMsg) is dict or type(pMsg) is collections.OrderedDict:
				l_msg = pprint.pprint(pMsg)
			#end if
			if self._show_line_no:
				frame 	= inspect.stack()[1]
				module 	= inspect.getmodule(frame[0])
				(l_frame, l_filename, l_line_number, l_function_name, l_lines, l_index) = inspect.getouterframes(inspect.currentframe())[1]
				l_newMsg = '{File:%s,Line:%s}--%s' % (l_filename, l_line_number, l_msg)
			else:
				l_newMsg = l_msg
			#end if
			
			self.__logger.debug(l_newMsg)
		#end if
#---------------------------------------------------------------------------------------------------------------------------------------------------		
	def show_only_debug_messages(self):
		self._only_debug_obj = OnlyDebugMessages()
		self.__logger.addFilter(self._only_debug_obj)
#---------------------------------------------------------------------------------------------------------------------------------------------------		
	def remove_show_only_debug_messages(self):
		if self._only_debug_obj:
			self.__logger.removeFilter(self._only_debug_obj)
#---------------------------------------------------------------------------------------------------------------------------------------------------		
	def show_line_no(self):
		self._show_line_no = True
#---------------------------------------------------------------------------------------------------------------------------------------------------		
	def remove_show_line_no(self):
		self._show_line_no = False
#------------------------------------------------------------------------------------------------------------------------
