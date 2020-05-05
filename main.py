# coding=utf-8
#
# Read China Life Trustee monthly reports (Excel format), convert to csv format
# for HTM price upload to Bloomberg AIM.
#
# For input and output file directories, check the config file.
# 
from trustee_report.utility import getCurrentDirectory
from itertools import chain, filterfalse
from functools import partial, reduce
from toolz.functoolz import compose
from utils.iter import divideToGroup, firstOf
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


	getPositionsFromLines = lambda lines: compose(
		partial(reduce, chain)
	  , partial(map, getPositionsFromSection)
	  , getSections
	)(lines)

	emptyLine = lambda line: len(line) == 0 or line[0] == ''


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
	  , partial(zip, headers)
	  , lambda _, line: \
	  		lognContinue('toPosition(): {0}'.format(line[0]), line)
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

	inputFile = join(getCurrentDirectory(), 'samples', '06 multiple cash multiple bond.xls')
	print(list(readFile(inputFile))[-1])