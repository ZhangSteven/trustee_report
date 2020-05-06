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



def outputCsv(date, positions):
	headerRows = \
		[ ['Upload Method', 'INCREMENTAL', '', '', '', '']
		, [ 'Field Id', 'Security Id Type', 'Security Id', 'Account Code'
		  , 'Numeric Value', 'Char Value']
		]

	toCsvRow = lambda p: \
		['CD012', 4, p['ISIN'], p['Portfolio'], p['AmortizedCost'], p['AmortizedCost']]


	return writeCsv( 'f3321tscf.htm.' + date + '.inc'
				   , chain(headerRows, map(toCsvRow, positions))
				   )



getInputFiles = lambda inputDirectory: compose(
	list
  , partial(map, lambda fn: join(inputDirectory, fn))
  , partial(filter, lambda fn: fn.endswith('.xls') or fn.endswith('.xlsx'))
  , getFiles
)(inputDirectory)



doOutput = lambda inputDirectory, date: compose(
	partial(outputCsv, date)
  , getHTMPositionsFromFiles
  , shownContinue
  , lambda files: \
  		lognRaise('no input files found under {0}'.format(inputDirectory)) \
  		if files == [] else files
  , lambda inputDirectory, _: getInputFiles(inputDirectory)
)(inputDirectory, date)



def shownContinue(L):
	for x in L:
		print(x)

	return L




if __name__ == '__main__':
	import logging.config
	logging.config.fileConfig('logging.config', disable_existing_loggers=False)
	
	import argparse
	parser = argparse.ArgumentParser(description='Process CL Trustee monthly reports')
	parser.add_argument( 'date', metavar='report_date', type=str
					   , help='date of the CL trustee reports in yyyy-mm-dd format')
	args = parser.parse_args()

	"""
	Put the CL Trustee monthly statements (Excel files) into the input directory
	(check config file), then do:

	$ python main.py <yyyy-mm-dd>

	Where the second argument is the report date. The output file is in the local
	directory.
	"""
	print('\nOutput File: {0}'.format(doOutput(getInputDirectory(), args.date)))