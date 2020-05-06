# coding=utf-8
#
# Read all Excel files from the input directory (see config file), convert them
# to csv format for HTM price upload to Bloomberg AIM.
# 

from trustee_report.report import getHTMPositionsFromFiles
from trustee_report.utility import getInputDirectory
from toolz.functoolz import compose
from utils.utility import writeCsv
from utils.file import getFiles
from functools import partial
from itertools import chain
from os.path import join
import logging
logger = logging.getLogger(__name__)



def showList(L):
	for x in L:
		print(x)

	return L



def lognRaise(msg):
	logger.error(msg)
	raise ValueError



"""
	[List] positions => [String] output csv file name

	Side effect: write a csv file in the local directory
"""
def outputCsv(positions):
	headerRows = \
		[ ['Upload Method', 'INCREMENTAL', '', '', '', '']
		, [ 'Field Id', 'Security Id Type', 'Security Id', 'Account Code'
		  , 'Numeric Value', 'Char Value']
		]

	toCsvRow = lambda p: \
		['CD012', 4, p['ISIN'], p['Portfolio'], p['AmortizedCost'], p['AmortizedCost']]

	return writeCsv( 'f3321tscf.htm.' + positions[0]['Date'] + '.inc'
				   , chain(headerRows, map(toCsvRow, positions))
				   )



# [String] inputDirectory => [List] excel files under that directory
getInputFiles = lambda inputDirectory: \
compose(
	list
  , partial(map, lambda fn: join(inputDirectory, fn))
  , partial(filter, lambda fn: fn.endswith('.xls') or fn.endswith('.xlsx'))
  , getFiles
)(inputDirectory)



"""
	[String] input directory => [String] output csv file name

	Side effect: write a csv file into the output directory
"""
doOutput = lambda inputDirectory: \
compose(
	outputCsv
  , list
  , getHTMPositionsFromFiles
  , showList
  , lambda files: \
  		lognRaise('no input files found under \'{0}\''.format(inputDirectory)) \
  		if files == [] else files
  , getInputFiles
)(inputDirectory)




if __name__ == '__main__':
	import logging.config
	logging.config.fileConfig('logging.config', disable_existing_loggers=False)

	"""
	Put the CL Trustee monthly statements (Excel files) into 'reports' directory,
	then do:

		$ python main.py

	The output file will be written to the local directory.
	"""
	print('\nOutput File: {0}'.format(doOutput(getInputDirectory())))