# coding=utf-8
# 
# The place to:
# 
# 1. Load configure files
# 2. Put functions shared by multiple modules.
# 

import os, configparser
import logging
logger = logging.getLogger(__name__)



def getCurrentDirectory():
	"""
	Get the absolute path to the directory where this module is in.

	This piece of code comes from:

	http://stackoverflow.com/questions/3430372/how-to-get-full-path-of-current-files-directory-in-python
	"""
	return os.path.dirname(os.path.abspath(__file__))



def _load_config(config_file='trustee_report.config'):
	"""
	Read the config file, convert it to a config object.
	"""
	cfg = configparser.ConfigParser()
	cfg.read(os.path.join(getCurrentDirectory(), config_file))
	return cfg



# initialized only once when this module is first imported by others
if not 'config' in globals():
	config = _load_config()



def getInputDirectory():
	global config
	return config['directory']['input']



def getOutputDirectory():
	global config
	return config['directory']['output']