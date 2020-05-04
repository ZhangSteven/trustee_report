# coding=utf-8
#
# Read Geneva reports (csv format), convert to data (list, dictionary).
#
# Input files:
#
# 1. Tax Lot Appraisal with Accruals
# 2. Cash Ledger
# 3. Dividend Receivable Payable
# 4. Profit and Loss (in different currencies)
# 5. Purchase and Sales
# 6. Broker Due To From
# 

from xlrd import open_workbook
from itertools import takewhile, chain, filterfalse, groupby
from functools import reduce, partial
from collections import namedtuple
from os.path import join
from utils.iter import pop, firstOf
from utils.excel import worksheetToLines
from utils.utility import fromExcelOrdinal
from toolz.dicttoolz import valmap
from toolz.functoolz import compose, flip
from toolz.itertoolz import groupby as groupbyToolz
import re
import logging
logger = logging.getLogger(__name__)



def lognRaise(msg):
	logger.error(msg)
	raise ValueError


def lognContinue(msg, x):
	logger.debug(msg)
	return x



toDateTimeString = lambda f: fromExcelOrdinal(f).strftime('%Y-%m-%d')
mapToList = lambda f, L: list(map(f, L))
mergeDictionary = lambda x, y: {**x, **y}
floatToIntString = lambda x: str(int(x)) if isinstance(x, float) else x
currencyString = lambda s: s[:-len(' Opening Balance')]



def getTaxlotInfo(file):
	"""
	[String] file => 
		( [Dictionary] metaData
		, [Dictionary] taxlotInfo (investId -> consolidated tax lot position)
		)

	Where each tax lot info object is a dictionary representing a consolidated
	position.

	NOTE: the dividend receivable cash is filtered out, because in the total
	NAV validation, we will sum each position's (market value + dvd/interest receivable
	+ dvd/interest payable) to calculate the total NAV and use that to compare
	with the final NAV. Therefore if a position has dividend receivable, it
	will reflect both as a cash entry and a dividend receivable for that position,
	causing double counting.

	To avoid that, we filter out dividend receivable item (usually a cash entry)
	in here.
	"""
	isCash = lambda p: p['ThenByDescription'] == 'Cash and Equivalents'

	isBondLike = lambda p: p['ThenByDescription'][-5:] == ' Bond' or \
							p['ThenByDescription'] == 'Fixed Deposit'

	costLocal = lambda p: p['Quantity'] if isCash(p) else \
					p['UnitCost']*p['Quantity']/100.0 if isBondLike(p) else \
					p['UnitCost']*p['Quantity']

	# [List] positions => ([Dictionary] fxRates
	getFxRatesFromPositions = compose(
		dict
	  , partial(map, lambda p: (getCurrency(p['SortByDescription']), p['MarketPrice']))
	  , partial(filter, isCash) 
	)

	"""
		[String] investment description => [String] investID

		In the tax lot report, there is no investId column, it is likely in
		between a pair of brackets in the investment description, like below:

		"CK INFRASTRUCTURE HOLDINGS L (1038 HK)", investID = "1038 HK"
		"Chinese Renminbi Yuan (CNY)", investID = "CNY"
		"CHMERC 6 03/21/22 (HK0000175916 HTM)", investID = "HK0000175916 HTM"

		But for a fixed deposit or a private fund, it can look like:

		"MSB Fixed Deposit 1.24 12/7/2019"

		In this case, investId = investment description
	"""
	idFromDescription = compose(
		lambda t: t[0].group(1) if t[0] != None else t[1]
	  , lambda description: (re.search('\((.*)\)', description), description) 
	)

	def updateTaxlot(fxRates, p):
		p['Currency'] = getCurrency(p['SortByDescription'])
		p['InvestID'] = idFromDescription(p['InvestmentDescription'])
		p['FX'] = 1.0 if p['Currency'] == p['BookCurrency'] else fxRates[p['Currency']]
		p['CostLocal'] = p['CostBook'] if p['Currency'] == p['BookCurrency'] else costLocal(p)
	
		return p

	# [Dictionary] fxRates, [List] positions 
	# 	=> [Dictionary] (investId -> [Dictionary] consolidatd position)
	processPositions = lambda fxRates, positions: \
	compose(
		partial(valmap, consolidate)
	  , partial(groupbyToolz, lambda p: p['InvestID'])
	  , partial(map, partial(updateTaxlot, fxRates))
	  , lambda _, positions: filterfalse( lambda p: p['TaxLotDescription'].endswith('DividendsReceivable')
	  									, positions)
	)(fxRates, positions)


	return \
	compose(
		lambda t: ( mergeDictionary(t[0], {'FX': t[1]})
				  , processPositions(t[1], t[2])
				  )
	  , lambda t: (t[0], getFxRatesFromPositions(t[1]), t[1])
	  , lambda t: (t[0], list(t[1]))
	  , getPositionsFromFile
	)(file)



def getProfitLoss(files):
	"""
	[List] files =>
		[Dictionary] metaData,
		[Dictionary] (invest id -> [Dictionary]) profitlossLocal (using investId
			to lookup profit loss position in the position's local currency)
		[List] profitloss (a list of dictionaries, each for a profit loss
			position in the portfolio's book currency),

	"""
	validated = lambda d1, d2: \
		all(d1[k] == d2[k] for k in ['PeriodEndDate', 'PeriodStartDate', 'Portfolio'])

	mergeMetaData = lambda d1, d2: \
		d1 if validated(d1, d2) else \
		lognRaise('getProfitLoss(): inconsistent meta data: {0}, {1}'.format(d1, d2))
	
	updateCurrency = lambda p: \
		mergeDictionary(p, {'Currency': getCurrency(p['Currency'])})

	mergePositions = lambda positions: reduce(
		lambda acc, el: \
			( mergeDictionary(acc[0], {el['Invest']: el}) \
				if el['BookCurrency'] == el['Currency'] else acc[0]
			, chain(acc[1], [el]) if el['BookCurrency'] == \
				getBookCurrency(el['Portfolio']) else acc[1]
			)
	  , positions
	  , ({}, [])
	)

	processPositions = compose(
		lambda t: (t[0], list(t[1]))
	  , mergePositions
	  , partial(map, updateCurrency)
	)


	return \
	compose(
		partial(
			reduce
		  , lambda acc, el: \
				( mergeMetaData(acc[0], el[0])
				, mergeDictionary(acc[1], el[1])
				, acc[2] + el[2]
				)
		)
	  , partial(map, lambda t: (t[0], *processPositions(t[1])))
	  , partial(map, getPositionsFromFile)

	)(files)



def getRawPositions(lines):
	"""
	[Iterable] lines => [Iterable] Raw Positions

	Where a raw position is a dictionary object.

	This is NOT a pure function, the first line is consumed when calling
	the function. All the lines up to the first empty line will be consumed
	when converting the iterable into a list.
	"""
	nonEmptyLine = lambda line: len(line) > 0 and line[0] != ''
	
	# [List] line => [List] headers (list of strings)
	getHeadersFromLine = compose(
		list
	  , partial(filterfalse, lambda s: s.strip() == '')
	  , partial(map, str)
	  , lambda line: lognContinue('getHeadersFromLine():', line)
	)

	return \
	compose(
		partial(map, dict)
	  , lambda t: map(partial(zip, getHeadersFromLine(t[0])), t[1])
	  , lambda lines: (pop(lines), lines)
	  , partial(takewhile, nonEmptyLine)
	)(lines)



def getPositions(lines):
	"""
	[Iterable] lines => [Dictionary] metaData, [Iterable] Positions

	Where metaData is a dictonary holding the report's meta data, such as date,
	book currency, etc.

	This is not a pure function.
	"""
	# consume all the lines up to the first empty line
	rawPositions = list(getRawPositions(lines))

	# only after those lines consumed can we get the proper meta data
	metaData = getReportInfo(lines)
	
	return metaData, map(partial(mergeDictionary, metaData), rawPositions)



def consolidate(group):
	"""
	[List] a group of tax lot positions => [Dictionary] consolidated position

	The consolidation adds up fields like quantity, costbook

	It also calculated the consolidated unit cost and the amortized cost (only
	makes sense for a HTM bond)
	"""
	position = group[0].copy()
	logger.debug('consolidate(): process {0}'.format(position['InvestID']))

	sumField = lambda field: sum(map(lambda d: d[field], group))

	def setFieldSum(field):
		position[field] = sumField(field)

	[setFieldSum(f) for f in \
		[ 'Quantity', 'CostBook', 'CostLocal', 'MarketValueBook', 'UnrealizedPriceGainLossBook'
		, 'UnrealizedFXGainLossBook', 'AccruedAmortBook', 'AccruedInterestBook']]

	position['UnitCost'] = 0 if position['Quantity'] == 0 else \
		sum(map(lambda p: p['Quantity']*p['UnitCost'], group))/position['Quantity']

	position['AmortizedCost'] = 0 if position['Quantity'] == 0 else \
		position['AccruedAmortBook']*100/(position['Quantity']*position['FX']) + position['UnitCost']

	return position



def getBookCurrency(portfolioId):
	"""
	[String] currency portfolioId => [String] book currency
	"""
	# FIXME: can we save the mapping into a configuration file?
	cMap = {
		'20051': 'HKD',
		'12229': 'HKD',
		'12366': 'HKD',
		'12549': 'HKD',
		'12630': 'HKD',
		'12734': 'HKD'
	}

	try:
		return cMap[portfolioId]
	except KeyError:
		logger.error('getBookCurrency(): {0} not supported'.format(portfolioId))
		raise ValueError



def getCurrency(description):
	"""
	[String] currency description => [String] currency
	"""
	# FIXME: can we save the mapping into a configuration file?
	cMap = {
		'Chinese Renminbi Yuan': 'CNY',
		'United States Dollar': 'USD',
		'Hong Kong Dollar': 'HKD'
	}

	try:
		return cMap[description]
	except KeyError:
		logger.error('getCurrency(): {0} not supported'.format(description))
		raise ValueError



def getReportInfo(lines):
	"""
	[Iterable] lines => [Dictionary] report meta data.

	Some properties are common to most Geneva reports, such as book currency,
	report end date, etc. We extract return them as a dictionary object.

	For a tax lot appraisal report, it will contain 4 keys, i.e., 'BookCurrency',
	'PeriodEndDate', 'PeriodStartDate' and 'Portfolio'.

	For a dividend receivable payable report, it will contain all the above
	except the 'PeriodStartDate' key.

	lines: rows in a file, where each row is a list of columns
	"""
	getDate = lambda n: fromExcelOrdinal(n).strftime('%Y-%m-%d')

	metaDataFunction = {
		'BookCurrency': lambda x: x,
		'PeriodEndDate': getDate,
		'PeriodStartDate': getDate,
		'Portfolio': floatToIntString
	}

	def buildMetaData(acc, el):
		if el[0] in metaDataFunction:
			acc[el[0]] = metaDataFunction[el[0]](el[1])	
		return acc

	return reduce(buildMetaData, lines, {})



def fileToLines(file):
	"""
	[String] file => [Iterable] lines

	Read an Excel file, convert its first sheet into lines, each line is
	a list of the columns in the row.
	"""
	return worksheetToLines(open_workbook(file).sheet_by_index(0))



"""
	[String] file => ([Dictionary] meta data, [Iterable] positions)
"""
getPositionsFromFile = compose(
	getPositions
  , fileToLines
)



"""
	[Fuction] processPositions 
	[String] file,
		=> ([Dictionary] meta data, [Object] process result of the positions)

	The function reads the file, get its meta data and positions, then calls
	the 'processPositions' funciton to process the positions, then returns
	the meta data and processed result. We create this function because it
	turns out to be the pattern when reading cash ledger, purchase sales and
	dividend receivable files.
"""
getPositionsProcessedFromFile = lambda processPositions, file: \
compose(
	lambda t: (t[0], processPositions(t[1]))
  , getPositionsFromFile
)(file)



"""
	[String] purchase sales report file
		=> (metadata, [List] positions from purchase sales report)
"""
# The first version works
# getPurchaseSales = compose(
# 	lambda t: (t[0], list(t[1]))
#   , lambda t: ( t[0]
#   			  , map( lambda p: mergeDictionary(
#   			  			p
#   			  		  , { 'TradeDate': toDateTimeString(p['TradeDate'])
#   			  			, 'TranID': floatToIntString(p['TranID'])
#   			  			}
#   			  		  )
# 				   , t[1])
#   			  )
#   , getPositionsFromFile
# )

# The second version works
# def getPurchaseSales(file):
# 	processPositions = compose(
# 		list
# 	  , partial( map
# 	  		   , lambda p: mergeDictionary(
# 		  			p
# 		  		  , { 'TradeDate': toDateTimeString(p['TradeDate'])
# 		  			, 'TranID': floatToIntString(p['TranID'])
# 		  			}
#   			  	 )
# 	  		   )
# 	)

# 	return \
# 	compose(
# 		lambda t: (t[0], processPositions(t[1]))
# 	  , getPositionsFromFile
# 	)(file)

# The third version
getPurchaseSales = partial(
	getPositionsProcessedFromFile
  , compose(
		list
	  , partial( map
	  		   , lambda p: mergeDictionary(
		  			p
		  		  , { 'TradeDate': toDateTimeString(p['TradeDate'])
		  			, 'TranID': floatToIntString(p['TranID'])
		  			}
  			  	 )
	  		   )
	)
)



"""
	[String] purchase sales report file
		=> (metadata, [List] positions from cash ledger report)
"""
getCashLedger = partial(
	getPositionsProcessedFromFile
  , compose(
		list
	  , partial( map
	  		   , lambda p: mergeDictionary(
  			  		p
  			  	  , { 'CashDate': toDateTimeString(p['CashDate'])
  			  		, 'Currency': getCurrency(currencyString(p['Currency_OpeningBalDesc']))
  			  		, 'TransID': floatToIntString(p['TransID'])
  			  		}
  			  	 )
	  		   )
	)
)



"""
	[String] dividend receivable payable report file 
		=> ( metaData
		   , [Dictionary] (investment -> 
		   		List (ExDate, LocalCurrency, LocalNetDividendRecPay, BookNetDividendRecPay))

	The reason to have a List value for each (key, value) pair in the resulting
	dictionary is because an investment can anounce multiple events. For example,
	an equity has a dividend and a special dividend, possibility in different
	currencies.
"""
getDividendReceivable = partial(
	getPositionsProcessedFromFile
  , compose(
		partial( valmap
			   , partial( mapToList
			   			, lambda p: ( toDateTimeString(p['EXDate'])
						  			, p['LocalCurrency']
						   			, p['LocalNetDividendRecPay']
						   			, p['BookNetDividendRecPay']
						   			)
			   			)
			   )
	  , partial(groupbyToolz, lambda p: p['Investment'])
	)
)



def getDueToFromBroker(file):
	"""
	[String] file (due to from broker report file)
		=> ( metaData
		   , [Dictionary] (tranId -> (date, currency, amount))

	Read the Due To From Broker report, get the accrued interest on the report 
	day.

	Note that we use the getPurchaseSales() function to read the file instead
	of the 
	"""
	processPositions = compose(
		dict
	  , partial( map
	  		   , lambda p: ( p['TranID']\
						   , (p['TradeDate'], p['Currency'], p['InterestPurchasedSold'])
						   )
	  		   )
	  , partial(filter, lambda p: p['TradeDate'] == p['PeriodEndDate'])
	)

	return \
	compose(
		lambda t: (t[0], processPositions(t[1]))
	  , getPurchaseSales
	)(file)




"""
	[Iterable] navPositions (positions from Statement of Net Assets report)
		=> ( [Dictionary] meta data
		   , [Float] total Assets
		   , [Float] net assets (NAV)
		   )
"""
getNavnAsset = compose(
	lambda t: ( t[0]
			  , sum(map( lambda p: p['SumBal']
			  		   , filter(lambda p: p['Segment0'] == 'Assets', t[1])))
			  , t[1][0]['SumBal1']
			  )
  , lambda t: (t[0], list(t[1]))
  , getPositionsFromFile
)



def loadSecurityMapping(fn):
	"""
	[String] fn (file name of the Bloomberg ID mapping file)
		=> [Dictionary] investId -> ( [String] Bloomberg FIGI
									, [String] security name
									)

	The function reads the file once and caches the result. Subsequent calls
	to the function only retrieves the cache.
	"""
	if not hasattr(loadSecurityMapping, 'mapping'):
		loadSecurityMapping.mapping = \
			dict(map( lambda p: (p['InvestID'], (p['Bloomberg FIGI'], p['Security Name']))\
					, getRawPositions(fileToLines(fn))))

	return loadSecurityMapping.mapping