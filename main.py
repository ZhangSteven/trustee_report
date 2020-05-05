# coding=utf-8
#
# Read China Life Trustee monthly reports (Excel format), convert to csv format
# for HTM price upload to Bloomberg AIM.
# 
from trustee_report.utility import getCurrentDirectory, getInputDirectory
from itertools import chain, filterfalse
from functools import partial, reduce
from toolz.functoolz import compose
from utils.iter import divideToGroup, firstOf
from utils.excel import worksheetToLines
from utils.utility import writeCsv
from utils.file import getFiles
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



def getHTMPositionsFromFiles(files):
	"""
	[Iterable] files (CL trustee excel files)
		=> [Iterable] HTM positions from these files, with ISIN code added to each
			position.
	"""

	def addISINCode(position):

		# some bond identifiers are not ISIN, map them to ISIN
		bondIsinMap = {
			'DBANFB12014':'HK0000175916',	# Dragon Days Ltd 6% 03/21/22
			'HSBCFN13014':'HK0000163607'	# New World Development 6% Sept 2023
		}
		
		getIdentifier = lambda p: p['Description'].split()[0]
		idToISIN = lambda id: bondIsinMap[id] if id in bondIsinMap else id

		return \
		compose(
			lambda isin: mergeDictionary(
			 	position
			  , {'ISIN': isin}
			)
		  , idToISIN
		  , getIdentifier
		)(position)
	# End of addISINCode()


	htmPositionsFromFile = compose(
		partial(map, addISINCode)
	  , partial(filter, lambda p: p['AssetType'] == 'HTMBond')
	  , readFile
	)


	return reduce(chain, map(htmPositionsFromFile, files))



def readFile(file):
	"""
	[String] file 
		=> [Iterable] Positions, each position is a dictionary containing
			the position's identifier, portfolio id and HTM price.
	"""

	def getPortfolioIdFromLines(lines):
		"""
		[Iterable] lines => [String] portfolio id

		Search for the line that contains the fund name and convert the
		name to portfolio id.
		"""

		# [Iterable] lines => [String] fund name (raise error if not found)
		getFundName = compose(
			lambda line: line[0][10:].strip()
		  , lambda line: lognRaise('getFundName(): failed get fund name') \
		  					if line == None else line
		  , partial( firstOf
		  		   , lambda line: isinstance(line[0], str) and \
								line[0].lower().startswith('fund name:'))
		)

		nameMap = \
		{ 'CLT-CLI HK BR (Class A-HK) Trust Fund  (Bond) - Par': '12229'
		, 'CLT-CLI HK BR (Class A-HK) Trust Fund  (Bond)': '12734'
		, 'CLT-CLI Macau BR (Class A-MC)Trust Fund (Bond)': '12366'
		, 'CLT-CLI Macau BR (Class A-MC)Trust Fund (Bond) - Par': '12549'
		, 'CLT-CLI HK BR (Class A-HK) Trust Fund - Par': '11490'
		, 'CLI Macau BR (Fund)': '12298'
		, 'CLI HK BR (Class G-HK) Trust Fund (Sub-Fund-Bond)': '12630'
		, 'CLI HK BR (Class G-HK) Trust Fund': '12341'
		}

		return \
		compose(
			lambda name: nameMap[name]
		  , lambda name: lognRaise('getPortfolioIdFromLines(): unsupported fund name {0}'.\
		  							format(name)) if not name in nameMap else name
		  , getFundName
		)(lines)
	# Enf of getPortfolioIdFromLines()


	getPositionsFromLines = lambda lines: compose(
		partial(reduce, chain)
	  , partial(map, getPositionsFromSection)
	  , getSections
	)(lines)


	emptyLine = lambda line: len(line) == 0 or all(map(lambda x: x == '', line))


	return \
	compose(
		lambda t: map(partial(mergeDictionary, {'Portfolio': t[0]}), t[1])
	  , lambda lines: ( getPortfolioIdFromLines(lines)
					  , getPositionsFromLines(lines)
					  )
  	  , partial(filterfalse, emptyLine)
	  , fileToLines
	  , lambda file: lognContinue('readFile(): {0}'.format(file), file)
	)(file)



"""
	[Iterable] lines => [List] sections

	Where is each section is a list of lines for a group 
"""
getSections = partial(
	divideToGroup
  , lambda line: re.match('[IVX]+\.\s+', line[0]) != None	# is it a section header line
)



def getPositionsFromSection(lines):
	"""
	[List] lines that belong to one section
		=> [Iterable] positions from that section
	"""

	# [String] section header (first cell in the first row of the section)
	# 	=> [String] asset type
	getAssetType = compose(
		lambda sectionHeader: \
			'Cash' if 'cash' in sectionHeader else \
			'Equity' if 'equities' in sectionHeader else \
			'Accruals' if 'accruals' in sectionHeader else \
			'HTMBond' if 'debt securities' in sectionHeader \
			and 'held for maturity' in sectionHeader else \
			'AFSBond' if 'debt securities' in sectionHeader \
			and 'available for sales' in sectionHeader else \
			'TradingBond' if 'debt securities' in sectionHeader \
			and 'held for trading' in sectionHeader else \
			lognRaise('getAssetType(): unsupported asset type: {0}'.format(sectionHeader))
	  
	  , lambda sectionHeader: sectionHeader.lower()
	)


	"""
		[Tuple] h (String, String) => [String] new header

		The original header is a String tuple, to make it easier for viewing
		and testing, we convert some of them to a single word. Actually for
		the purpose of HTM price uploading, we only need to convert the column
		for HTM amortized cost ('AmortizedPrice') column.
	"""
	toNewHeader = lambda h: \
		'Description' if h[2].startswith('Description') else \
		'Currency' if (h[0], h[2]) == ('', 'CCY') else \
		'Cost' if (h[0], h[2]) == ('', 'Cost') else \
		'MarketValue' if h in [('Total', 'Mkt', 'Value'), ('Total', 'M.', 'Value')] else \
		'MarketPrice' if (h[1], h[2]) == ('Market', 'Price') else \
		'AmortizedCost' if (h[1], h[2]) == ('Amortized', 'Price') else \
		'Quantity' if (h[0], h[2]) == ('', 'Share') or (h[1], h[2]) == ('Par', 'Amt') else h


	getHeaders = lambda line1, line2, line3: compose(
		list
	  , partial(map, toNewHeader)
	  , zip
	)(line1, line2, line3)

	
	toPosition = lambda headers, line: compose(
		dict
	  , partial(filterfalse, lambda t: t[0] == ('', '', ''))
	  , partial(zip, headers)
	  , lambda _, line: \
	  		lognContinue('toPosition(): {0}'.format(line[0]), line)
	)(headers, line)


	countEmptyDictValue = lambda d: sum(1 if d[key] == '' else 0 for key in d)
	unwantedPosition = lambda p: countEmptyDictValue(p) > 2


	"""
	In portfolio 12630, there are multiple entries for one position. We need to
	group those together and create a consolidated position.
	"""
	toPositionGroups = partial(divideToGroup, lambda p: p['Description'] != '')


	return \
	compose(
		partial(map, partial(mergeDictionary, {'AssetType': getAssetType(lines[0][0])}))
	  , partial(map, consolidatePositionGroup)
	  , toPositionGroups
	  , partial(filterfalse, unwantedPosition)
	  , partial( map
	  		   , partial(toPosition, getHeaders(lines[1], lines[2], lines[3])))
	  , lambda lines: lines[4:]
	  , lambda lines: lognContinue( 'getPositionsFromSection(): {0}'.format(lines[0][0])
	  							  , lines)
	)(lines)



def consolidatePositionGroup(group):
	"""
	[List] group (a group of positions of the same security)
		=> [Dictionary] consolidated position

	NOTE: the result is not a truly consolidated position because it only adds
	up the quantity and create a weighted average of the amortized cost (if any).

	This is good enough if we only need amortized cost of HTM bond positions and
	can ignore other positions. Modification is needed if we need other positions
	in the future.
	"""
	position = group[0].copy()

	if 'Quantity' in position:
		position['Quantity'] = sum(map(lambda p: p['Quantity'], group))
		if 'AmortizedCost' in position:
			position['AmortizedCost'] = \
				sum(map( lambda p: p['Quantity']*p['AmortizedCost']
					   , group)
				   )/position['Quantity']

	return position



"""
	[String] file => [Iterable] lines

	Read an Excel file, convert its first sheet into lines, each line is
	a list of column values in that row.
"""
fileToLines = compose(
	worksheetToLines
  , lambda file: open_workbook(file).sheet_by_index(0)
)



def outputCsv(date, positions):
	headerRows = \
		[ ['Upload Method', 'INCREMENTAL', '', '', '', '']
		, [ 'Field Id', 'Security Id Type', 'Security Id', 'Account Code'
		  , 'Numeric Value', 'Char Value']
		]

	toCsvRow = lambda p: \
		['CD012', 4, p['ISIN'], p['Portfolio'], p['AmortizedCost'], p['AmortizedCost']]


	return writeCsv( join(getCurrentDirectory(), 'f3321tscf.htm.' + date + '.inc')
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