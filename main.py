# coding=utf-8
#
# Read China Life Trustee monthly reports (Excel format), convert to upload format
# for HTM price uploading to Bloomberg AIM.
#
# For input and output file directories, check the config file.
# 
from trustee_report.utility import getCurrentDirectory
from itertools import chain, filterfalse
from functools import partial, reduce
from toolz.functoolz import compose
from utils.iter import divideToGroup
from utils.excel import worksheetToLines
from xlrd import open_workbook
from os.path import join
import re
import logging
logger = logging.getLogger(__name__)



def lognContinue(msg, x):
	logger.debug(msg)
	return x



def lognRaise(msg):
	logger.error(msg)
	raise ValueError



mergeDictionary = lambda d1, d2: \
	{**d1, **d2}



def readFile(file):
	"""
	[String] file 
		=> [Iterable] Positions, each position is a dictionary containing
			the position's identifier, portfolio id and HTM price.
	"""
	# FIXME: Add implementation
	getPortfolioIdFromLines = lambda lines: '88888'

	getPositionsFromLines = lambda lines: compose(
		partial(reduce, chain)
	  , partial(map, getPositionsFromSection)
	  , getSections
	)(lines)


	return \
	compose(
		lambda t: map(partial(mergeDictionary, {'Portfolio': t[0]}), t[1])
	  , lambda lines: ( getPortfolioIdFromLines(lines)
					  , getPositionsFromLines(lines)
					  )
	  , fileToLines
	  , lambda file: lognContinue('readFile(): {0}'.format(file), file)
	)(file)



def getSections(lines):
	"""
	[Iterable] lines => [List] sections

	Where is each section is a list of lines for a group 
	"""
	isSectionHeaderLine = lambda line: \
		False if len(line) == 0 else re.match('[IVX]+\.\s+', line[0]) != None

	emptyLine = lambda line: len(line) == 0 or line[0] == ''

	return \
	compose(
		partial(divideToGroup, isSectionHeaderLine)
	  , partial(filterfalse, emptyLine)
	)(lines)
	


def getPositionsFromSection(lines):
	"""
	[List] lines that belong to one section
		=> [Iterable] positions from that section
	"""
	getAssetType = compose(
		lambda sectionHeader: \
			'Cash' if 'cash' in sectionHeader else \
			'Equity' if 'equities' in sectionHeader else \
			'Accruals' if 'accruals' in sectionHeader else \
			'HTMBond' if 'debt securities' in sectionHeader \
			and 'held for maturity' in sectionHeader else \
			'AFSBond' if 'debt securities' in sectionHeader \
			and 'available for sales' in sectionHeader else \
			lognRaise('getAssetType(): unsupported asset type: {0}'.format(sectionHeader))
	  
	  , lambda sectionHeader: sectionHeader.lower()
	)

	toNewHeader = lambda h: \
		'Description' if h[1].startswith('Description') else \
		'Currency' if h[1] == 'CCY' else \
		'Cost' if h[1] == 'Cost' else \
		'MarketValue' if h in [('Mkt', 'Value'), ('M.', 'Value')] else \
		'MarketPrice' if h == ('Market', 'Price') else \
		'AmortizedPrice' if h == ('Amortized', 'Price') else h


	getHeaders = lambda line1, line2: compose(
		list
	  , partial(map, toNewHeader)
	  , zip
	)(line1, line2)
	
	toPosition = lambda headers, line: compose(
		dict
	  , partial(filterfalse, lambda t: t[0] == ('', ''))
	  , zip
	)(headers, line)


	return \
	compose(
		partial(map, partial(mergeDictionary, {'AssetType': getAssetType(lines[0][0])}))
	  , partial( map
	  		   , partial(toPosition, getHeaders(lines[1], lines[2])))
	  , lambda lines: lines[3:]
	  , lambda lines: lognContinue( 'getPositionsFromSection(): {0}'.format(lines[0][0])
	  							  , lines)
	)(lines)




"""
	[String] file => [Iterable] lines

	Read an Excel file, convert its first sheet into lines, each line is
	a list of column values in that row.
"""
fileToLines = compose(
	worksheetToLines
  , lambda file: open_workbook(file).sheet_by_index(0)
)




if __name__ == '__main__':
	import logging.config
	logging.config.fileConfig('logging.config', disable_existing_loggers=False)

	inputFile = join(getCurrentDirectory(), 'samples', '05 cash multiple bond.xls')
	for p in readFile(inputFile):
		print(p)
		break