# coding=utf-8
#
# Read Geneva reports to generate CLAMC data reports. See __main__ section
# for more details.
# 
from clamc_datafeed.feeder import getTaxlotInfo, getProfitLoss, getCashLedger \
								, mergeDictionary, getDividendReceivable \
								, getPurchaseSales, getNavnAsset, getCurrency \
								, floatToIntString, loadSecurityMapping \
								, getDueToFromBroker
from clamc_datafeed.utility import getDataDirectory, getOutputDirectory \
								, getHistoricalFX, timestampnow
from clamc_datafeed.position import getClamcAccountId
from clamc_datafeed.trade import handleTrade
from clamc_datafeed.mysql import lookupWithInvestId, addnLookup

from utils.iter import firstOf
from utils.utility import writeCsv
from utils.file import getFiles
from toolz.functoolz import compose, flip
from functools import partial, reduce
from itertools import chain, filterfalse
from datetime import datetime
from os.path import join
import re, csv
import logging
logger = logging.getLogger(__name__)



class InvalidMetaData(Exception):
	pass


def lognRaise(msg):
	logger.error(msg)
	raise ValueError


def lognContinue(msg, x):
	logger.debug(msg)
	return x



isHTMBondfromInvestId = lambda investId: \
	len(investId) == 16 and investId[-4:] == ' HTM'



"""
	[String] invest id => 
		[String] id with out the ' HTM' suffix if it is a HTM bond invest id,
					otherwise the invest id is not changed.
"""
removeHTMFromId = lambda x: x.split()[0] if isHTMBondfromInvestId(x) else x



"""
	Map Bloomberg exchange code to exchange code defined in ACC_POSITION_INFO, 
	field 'EXCH_CD'.

	FIXME: For US stocks, Bloomberg exchange code is all "US". There is no way to
	further differentiate between NASDAQ and NYSE. Here we map all "US" stocks
	to NYSE (code 16).
"""
mapExchangeCode = lambda exchange: {'HK': '10', 'US': '16', 'SP': '15'}[exchange]



"""
	[String] bankName => [String] bank code

	Map the bank name to its code.
"""
getBankCode = lambda bankName: \
	{
		'BOCOM'  : '57867311',	# bank of communications
		'BOCOMHK': '57867311',	# same as above
		'BOCHK'  : '19361134',	# bank of china hong kong
		'BOC'    : '19361134',	# same as above
		'MSB'    : '30205426',	# minsheng bank
		'CMBC'   : '30205426',	# also minsheng bank	
		'IB'	 : '40617175',	# industrial bank
		'CIB'	 : '40617175'	# also industrial bank
	}[bankName.upper()]



getBankCodeFromCurrency = lambda currency: \
	getBankCode(
		{ 'USD': 'BOCHK'
		, 'HKD': 'CMBC'
		, 'CNY': 'BOCHK'
		}[currency]
	)



"""
	[Dictionary] p (from cash ledger report) 
		=> [Bool] is it a deposit or withdrawal
"""
isDepositWithdrawal = lambda p:	\
	p['TranDescription'] in ['Deposit', 'Withdraw']



"""
	[Dictionary] position (from cash ledger report)
		=> [Dictioanry] p (for ACC_CAPTL_CHG_INFO table)
"""
cashInOut = lambda position: \
	{ 'BIZ_DT': position['CashDate']\
	, 'ACC_CD': getClamcAccountId(position['Portfolio'])\
	, 'TX_DIRECTION_CD': '1' if position['LocalAmount'] > 0 else '-1'\
	, 'CURR_CD': getCurrency(position['Investment'])\
	, 'EXCH_RATE': position['BookAmount']/position['LocalAmount']\
	, 'TX_AMT_BS': abs(position['BookAmount'])\
	, 'TX_AMT_NTV': abs(position['LocalAmount'])\
	, 'TX_SHARE': 0\
	, 'ENTRY_TIME': timestampnow()\
	}



"""
	[Iterable] cash ledger (from cash ledger report)
		=> [Iterable] rows for ACC_CAPTL_CHG_INFO table
"""
handleCashFlow = compose(
	partial(map, cashInOut)
  , partial(filter, lambda p: p['CashDate'] == p['PeriodEndDate'])
  , partial(filter, isDepositWithdrawal)
)



"""
	[Dictionary] metaTaxLot (meta data)
	[Float] nav
	[Float] asset
	[Float] total gainloss from the profit loss report
		=> [Dictionary] row (for the ACC_NET_VALUE_INFO table)
"""
toNavRow = lambda metaTaxlot, nav, asset, totalGain: \
	{ 'BIZ_DT': metaTaxlot['PeriodEndDate']
	, 'ACC_CD': getClamcAccountId(metaTaxlot['Portfolio'])
	, 'CURR_CD': metaTaxlot['BookCurrency']
	, 'EXCH_RATE': metaTaxlot['FX']['CNY']
	, 'TOTAL_NAV_CNY': nav/metaTaxlot['FX']['CNY']
	, 'TOTAL_NAV_BS': nav
	, 'TOTAL_SHARE': 0
	, 'UNIT_NAV_CNY': 0
	, 'UNIT_NAV_BS': 0
	, 'TOT_ASSET_CNY': asset/metaTaxlot['FX']['CNY']
	, 'TOT_ASSET_BS': asset
	, 'TOTAL_GAIN_CNY': totalGain/metaTaxlot['FX']['CNY']
	, 'TOTAL_GAIN_BS': totalGain
	, 'ENTRY_TIME': timestampnow()
	}



def toSecurityMapping(secMap, position):
	"""
	[Dictionary] secMap,
	[Dictionary] position from profit and loss report
		=> [Dictionary] a position in SEC_MAP_INFO table

	Assume only listed security position as the input
	"""
	isEquity = lambda p: not (p['PrintGroup'] in ['Corporate Bond', 'Open-End Fund'])

	return \
	{ 'INNER_SEC_CD': position['Invest']
	, 'SEC_NM': secMap[removeHTMFromId(position['Invest'])][1]
	, 'ISIN': ''
	, 'SEDOL': ''
	, 'CUSIP': ''
	, 'SEC_TX_CD': position['Invest'].split()[0] if isEquity(position) else ''
	, 'SEC_EXCH_CD': mapExchangeCode(position['Invest'].split()[-1]) \
						if isEquity(position) else '04' 
	, 'AIM_INNER_SEC_CD': secMap[removeHTMFromId(position['Invest'])][0]
	, 'ENTRY_TIME': timestampnow()
	}



"""
	[String] investId,
	[String] investType ('PrintGroup' property of profit loss report, or
						 'henByDescription' property of tax lot report)
"""
getAccountingCode = lambda investId, investType: \
	'6' if investType in ['Cash and Equivalents', 'Fixed Deposit'] \
	else '5' if isHTMBondfromInvestId(investId) else '3'



"""
	[String] investId,
	[String] investType ('PrintGroup' property of profit loss report, or
						 'henByDescription' property of tax lot report)
"""
getExchangeCode = lambda investId, investType: \
	mapExchangeCode(investId.split()[-1]) if investType in \
	['Common Stock', 'Real Estate Investment Trust', 'Stapled Security'] else '04'
	


isAltFixedDeposit = lambda altPosition: altPosition['PARTY_TP_CD'] == '7'


"""
	[List] altPositions => [Iterable] altPositions with fund info added

	When we write the ALT_PARTY_INFO table, each alternative fund has two lines,
	only is included in the altPosition. Therefore we need to add another for
	those funds.

	Assume there are only two types of alternative investments: fixed deposit
	and private fund.
"""
addAltPartyInfoFund = lambda altPositions: \
	chain( filter(isAltFixedDeposit, altPositions)
		 , reduce(chain, map( lambda fd: (fd, mergeDictionary(fd, {'PARTY_TP_CD': '8'}))
		 					, filterfalse(isAltFixedDeposit, altPositions))))



def processReport( taxLotFile, cashLedgerFile, dvdReceivableFile, profitlossFiles\
				 , purchaseSalesFile, dueToFromFile, navFile, lastYearEndNavFile=None
				 , securityMappingFile='Bloomberg FIGI.xlsx'):
	"""
	Process Geneva reports and output data to be written to csv.
	"""
	logger.info('processReport(): start')

	metaTaxlot, taxlot = getTaxlotInfo(taxLotFile)
	metaCashLedger, cashLedger = getCashLedger(cashLedgerFile)
	metaDividendReceivable, dividendReceivable = getDividendReceivable(dvdReceivableFile)
	metaProfitLoss, plLocal, plPositions = getProfitLoss(profitlossFiles)
	metaPurchaseSales, purchaseSales = getPurchaseSales(purchaseSalesFile)
	metaDueToFrom, dueToFrom = getDueToFromBroker(dueToFromFile)
	metaNAV, asset, nav = getNavnAsset(navFile)

	validateMetaData( metaTaxlot, metaCashLedger, metaDividendReceivable\
					, metaProfitLoss, metaPurchaseSales, metaDueToFrom\
					, metaNAV)
	
	checkProfitLossPrintGroup(plPositions)
	checkProfitLossBaseCurrency(plLocal, plPositions)
	checkMissingMarketPrice(taxlot)
	checkMissingSecurityMapping(plPositions, 'Bloomberg FIGI.xlsx')

	outputPositionInfo = reduce( 
		lambda acc, el: \
			mergeDictionary( acc
						   , {el['Invest']: position(metaTaxlot, taxlot \
								, purchaseSales, dividendReceivable, plLocal, el)}
						   )
	  , plPositions
	  , {}
	)

	checkInconsistentNAV(outputPositionInfo.values(), nav)
	checkNAVwithLastYear( nav
						, outputPositionInfo.values()
						, cashLedger
						, getLastYearEndNavFile(metaTaxlot['PeriodEndDate']) \
							if lastYearEndNavFile is None else lastYearEndNavFile
						)


	return metaTaxlot['PeriodEndDate']\
		 , outputPositionInfo.values() \
		 , list(map( partial(altPosition, cashLedger, purchaseSales)
		 		   , filter(isAlternative, plPositions)))\
 		 , handleCashFlow(cashLedger)\
		 , handleTrade( outputPositionInfo
		 			  , dueToFrom
		 			  , dict(map(lambda p: (p['Description'], p['Invest']), plPositions))
		 			  , purchaseSales
		 			  , cashLedger)\
		 , toNavRow(metaTaxlot, nav, asset, totalGainLoss(outputPositionInfo.values()))\
		 , map( partial(toSecurityMapping, loadSecurityMapping(securityMappingFile))
		 	  , filter(isListedSecurity, plPositions))



"""
	[List] plPositions (from the profit loss report)
		=> 0 if the profit loss report's PrintGroup column has a problem.
		   raise ValueError

	Sometimes Geneva profit loss reports are generated with a strange condition,
	where the 'PrintGroup' column is populated with currency instead of the
	asset type. We need to detect this condition.
"""
checkProfitLossPrintGroup = lambda plPositions: \
	lognRaise('checkProfitLossPrintGroup(): PrintGroup column has a problem') \
		if any(p['PrintGroup'] in ['Chinese Renminbi Yuan', 'Hong Kong Dollar', 'United States Dollar'] for p in plPositions) \
		else 0



"""
	[Dictionary] plLocal (investId -> [Dictionary] plPosition in local currency),
	[List] plPositions in the report's base currency
		=> set of base currencies that are missing
"""
missingProfitLossBaseCurrency = compose(
	set
  , partial(map, lambda p: p['Currency'])
  , lambda plLocal, plPositions: \
  		filterfalse(lambda p: p['Invest'] in plLocal, plPositions)
)



"""
	[Dictionary] plLocal (investId -> [Dictionary] plPosition in local currency),
	[List] plPositions in the report's base currency
		=> 0 if each profit loss base currency position has a value in the plLocal
			raise ValueError otherwise
"""
checkProfitLossBaseCurrency = compose(
	lambda s: lognRaise('checkProfitLossBaseCurrency(): missing base currency: {0}'.\
						format(', '.join(s))) if len(s) > 0 else 0
  , lambda t: missingProfitLossBaseCurrency(t[0], t[1])
  , lambda plLocal, plPositions: \
  		lognRaise('checkProfitLossBaseCurrency(): missing portfolio base currency') \
  		if len(plPositions) == 0 else (plLocal, plPositions)
)



"""
	[List] positions for the ACC_POSITION_INFO table,
	[Float] nav (from nav report)
		=> 0 if nav is consistent or,
		   raise ValueError
"""
checkInconsistentNAV = compose(
	lambda delta: \
		lognRaise('checkInconsistentNAV(): delta too big: {0}'.format(delta)) \
		if abs(delta) > 0.01 else 0

  , lambda positions, nav: \
  		nav - \
  		sum(map( lambda p: p['FIN_VALUAT_BS'] + p['ACCR_INTE_BS'] \
  					+ p['RECV_BONUS_BS'] - p['INTE_PAYABLE_BS']
  			   , positions))
)



"""
	[Iterable] positions (all positions from the ALT_BASE_INFO table)
		=> [Float] gain/loss from all positions
"""
totalGainLoss = compose(
	sum
  , partial( map
		   , lambda p: p['FAIR_VALU_CHG_PL_BS'] + p['SPREAD_GAIN_BFO_TAX_BS']\
				+ p['SPREAD_GAIN_VAT_BS'] + p['INTE_INCOME_BFO_TAX_BS']\
				+ p['INTE_INCOME_VAT_BS'] + p['INTE_EXPNS_BS']\
		  		+ p['OTHER_PL_BS'] + p['PL_ADJUST_BS']
		   )
)



"""
	[Iterable] cash ledger (from cash ledger report)
		=> [Float] net cash flow from the report
"""
totalCashFlow = compose(
	sum
  , partial(map, lambda p: p['BookAmount'])
  , partial(filter, lambda p: p['CashDate'] >= p['PeriodEndDate'][0:4] + '-01-01')
  , partial(filter, isDepositWithdrawal)
)



"""
	[Float] nav (from the nav report)
	[Iterable] positions for the ACC_POSITION_INFO table,
	[Iterable] cash ledger (from cash ledger report)
	[String] NAV file as of last year end
		=> 0 if nav is consistent or,
		   raise ValueError
"""
checkNAVwithLastYear = compose(
  	lambda x: \
		lognRaise('checkNAVwithLastYear(): nav delta: {0}'.format(x)) \
		if abs(x) > 0.01 else 0
  , lambda t: t[0] - t[1] - t[2] - t[3]
  , lambda nav, positions, cashLedger, navFileLastYear: \
  		( nav
  		, totalGainLoss(positions) 
  		, totalCashFlow(cashLedger)
  		, getNavnAsset(navFileLastYear)[2]
  		)
)



"""
	[Iterable] plPositions (from profit and loss report)
	[String] security mapping file
		=> 0 if no missing mappings or,
		   raise ValueError
"""
checkMissingSecurityMapping = compose(
	lambda L: lognRaise('checkMissingSecurityMapping(): {0}'.format(', '.join(L))) \
				if L != [] else 0
  , lambda t: list(set(t[0]) - set(loadSecurityMapping(t[1]).keys()))
  , lambda plPositions, fn: (getListedSecurityIds(plPositions), fn)
)



"""
	[Dictonary] p (a position from profit and loss report)
		=> [Bool] is this a liste security to be put into the SEC_MAP_INFO table
"""
isListedSecurity = lambda p: \
	not (p['PrintGroup'] == 'Cash and Equivalents' or isAlternative(p))



"""
	[Iterable] plPositions (from profit and loss report) 
		=> [Iterable] list of invest Ids that are listed in Bloomberg
"""
getListedSecurityIds = compose(
	partial(map, removeHTMFromId)
  , partial(map, lambda p: p['Invest'])
  , partial(filter, isListedSecurity)
)



"""
	[Dictonary] taxlot (from tax lot report)
		=> [List] list of invest Ids that should mark to market but doesn't have
			a price.
"""
missingMarketPrice = compose(
    list
  , partial(map, lambda p: p['InvestID'])
  , partial(filter, lambda p: p['MarketPrice'] == 'NA')
  , partial( filterfalse
		   , lambda p: p['ThenByDescription'] in ['Cash and Equivalents', 'Fixed Deposit'] \
		   		or p['Quantity'] == 0 or isHTMBondfromInvestId(p['InvestID']))
  , lambda taxlot: taxlot.values()
)



"""
	[Dictonary] taxlot (from tax lot report)
		=> 0 if nothing is wrong, or
		   raise ValueError if missing market price is detected.

	The reason to have checkMissingMarketPrice() and missingMarketPrice() is
	because missingMarketPrice() is easier to test.
"""
checkMissingMarketPrice = compose(
	lambda L: lognRaise('checkMissingMarketPrice(): lack market price: {0}'.format(', '.join(L))) \
				if L != [] else 0
  , missingMarketPrice
)



def validateMetaData( metaTaxlot, metaCashLedger, metaDividendReceivable\
					, metaProfitLoss, metaPurchaseSales, metaDueToFrom\
					, metaNAV):
	"""
	Validate meta of the reports, 3 rules:

	(1) All reports have the same period end date and same portfolio.
	(2) purchase sales, cash ledger starts from 1950-01-01
	(3) realized GL starts from the year beginning.
	"""
	portIdnDates = \
		list(map( lambda m: (m['Portfolio'], m['PeriodEndDate'])\
			   	, filter( lambda m: len(m) > 0\
			   		  	, [ metaTaxlot, metaCashLedger, metaDividendReceivable\
			   		  	  , metaProfitLoss, metaPurchaseSales, metaDueToFrom\
			   		  	  , metaNAV])))

	if not all(el == portIdnDates[0] for el in portIdnDates):
		logger.error('validateMetaData(): inconsistent portfolio id or dates: {0}'\
						.format(portIdnDates))
		raise InvalidMetaData

	if metaPurchaseSales['PeriodStartDate'] != '1950-01-01':
		logger.error('validateMetaData(): invalid purchase sales start date: {0}'\
						.format(metaPurchaseSales['PeriodStartDate']))
		raise InvalidMetaData

	if metaCashLedger['PeriodStartDate'] != '1950-01-01':
		logger.error('validateMetaData(): invalid cash ledger start date: {0}'\
						.format(metaCashLedger['PeriodStartDate']))
		raise InvalidMetaData

	if metaProfitLoss['PeriodStartDate'] != metaTaxlot['PeriodEndDate'][0:4] + '-01-01':
		logger.error('validateMetaData(): invalid profit loss start date: {0}'\
						.format(metaProfitLoss['PeriodStartDate']))
		raise InvalidMetaData



"""
	[Dictionary] p (position from a profit loss report)
	=> [Bool] is it an alternative investment position
"""
isAlternative = lambda p: \
	p['PrintGroup'] == 'Fixed Deposit' or \
	p['PrintGroup'] == 'Open-End Fund' and not p['Invest'] == 'CLFLDIF HK'



def position(metaTaxlot, taxlot, ps, dividendReceivable, plLocal, plPosition):
	"""
	[Dictionary] metaTaxlot (meta data of tax lot, including FX)
	[Dictionary] (invest id -> [Dictionary] tax lot position info) taxlot
	[List] ps (purchase sales records since inception)
	[Dictionary] (invest id -> [Dictionary] profit loss local) plLocal
	[Dictionary] plPosition from profit and loss report, in book currency
		=> [Dictionary] position in ACC_POSITION_INFO table
	"""
	logger.debug('position(): {0}'.format(plPosition['Invest']))

	isHTMBond = lambda p: isHTMBondfromInvestId(p['Invest'])
	unrealizedGL = lambda p: p['UnrealizedPrice'] + p['UnrealizedFX'] + p['UnrealizedCross']
	realizedGL = lambda p: p['RealizedPrice'] + p['RealizedFX'] + p['RealizedCross']
	intDvd = lambda p: p['Interest'] + p['Dividend']

	cp = {}
	cp['BIZ_DT'] = plPosition['PeriodEndDate']
	cp['ACC_CD'] = getClamcAccountId(plPosition['Portfolio'])
	cp['ACCTING_CLA_CD'] = getAccountingCode(plPosition['Invest'], plPosition['PrintGroup'])

	cp['CURR_CD'] = plPosition['Currency']
	cp['SEC_CD'] = '99101' + getBankCodeFromCurrency(plPosition['Currency']) \
		if plPosition['PrintGroup'] == 'Cash and Equivalents' \
		else getOrCreateSecurityId(plPosition['Invest'], ps) \
		if isAlternative(plPosition) else plPosition['Invest']

	cp['EXCH_CD'] = getExchangeCode(plPosition['Invest'], plPosition['PrintGroup'])

	cp['LONG_SHORT_FLAG'] = '1' if plPosition['EndingQuantity'] >= 0 else '-1'
	cp['HOLD_QTY'] = 0 if isAlternative(plPosition) or plPosition['EndingQuantity'] == 0 \
						else taxlot[plPosition['Invest']]['Quantity']

	cp['PURCH_COST_BS'] = 0 if plPosition['EndingQuantity'] == 0 else \
		taxlot[plPosition['Invest']]['MarketValueBook'] if isHTMBond(plPosition) \
		else taxlot[plPosition['Invest']]['CostBook']

	cp['PURCH_COST_NTV'] = 0 if plPosition['EndingQuantity'] == 0 else \
		taxlot[plPosition['Invest']]['AmortizedCost']*plPosition['EndingQuantity']/100 \
		if isHTMBond(plPosition) else taxlot[plPosition['Invest']]['CostLocal']

	cp['PREM_DISC_NTV'] = 0 if plPosition['EndingQuantity'] == 0 or not isHTMBond(plPosition) else \
		(100 - taxlot[plPosition['Invest']]['AmortizedCost'])*plPosition['EndingQuantity']/100

	cp['PREM_DISC_BS'] = 0 if plPosition['EndingQuantity'] == 0 else \
		cp['PREM_DISC_NTV'] * taxlot[plPosition['Invest']]['FX']

	cp['FIN_VALUAT_BS'] = 0 if plPosition['EndingQuantity'] == 0 else \
		taxlot[plPosition['Invest']]['MarketValueBook']

	cp['FIN_VALUAT_NTV'] = 0 if plPosition['EndingQuantity'] == 0 else \
		cp['FIN_VALUAT_BS']/taxlot[plPosition['Invest']]['FX']

	cp['FLOAT_PL_BS'] = 0 if plPosition['EndingQuantity'] == 0 \
		or isHTMBond(plPosition) or isAlternative(plPosition) else \
				taxlot[plPosition['Invest']]['UnrealizedPriceGainLossBook'] \
				+ taxlot[plPosition['Invest']]['UnrealizedFXGainLossBook'] \
				+ taxlot[plPosition['Invest']]['AccruedAmortBook']

	cp['FLOAT_PL_NTV'] = 0 if cp['FLOAT_PL_BS'] == 0 or plPosition['PrintGroup'] == 'Cash and Equivalents' \
		else cp['FLOAT_PL_BS']/taxlot[plPosition['Invest']]['FX']

	cp['FAIR_VALU_CHG_PL_BS'] = 0 if plPosition['EndingQuantity'] == 0 or isHTMBond(plPosition) \
		or isAlternative(plPosition) else unrealizedGL(plPosition)

	cp['FAIR_VALU_CHG_PL_NTV'] = 0 if cp['FAIR_VALU_CHG_PL_BS'] == 0 else \
		unrealizedGL(plLocal[plPosition['Invest']])

	cp['ACCR_INTE_BS'] = 0 if not plPosition['Invest'] in taxlot else \
		taxlot[plPosition['Invest']]['AccruedInterestBook']
	cp['ACCR_INTE_NTV'] = 0 if cp['ACCR_INTE_BS'] == 0 else \
		cp['ACCR_INTE_BS']/taxlot[plPosition['Invest']]['FX']

	cp['RECV_BONUS_BS'], cp['RECV_BONUS_NTV'] = \
		getLocalBookSum(plPosition['Description'], plPosition['Currency'], dividendReceivable)

	cp['INTE_PAYABLE_BS'] = 0
	cp['INTE_PAYABLE_NTV'] = 0
	cp['IMPA_PROV_BS'] = 0
	cp['IMPA_PROV_NTV'] = 0
	cp['IMPA_LOS_BS'] = 0
	cp['IMPA_LOS_NTV'] = 0

	cp['SPREAD_GAIN_BFO_TAX_BS'] = realizedGL(plPosition) if plPosition['EndingQuantity'] != 0 \
		else realizedGL(plPosition) + unrealizedGL(plPosition)

	cp['SPREAD_GAIN_BFO_TAX_NTV'] = realizedGL(plLocal[plPosition['Invest']]) if plPosition['EndingQuantity'] != 0 \
		else realizedGL(plLocal[plPosition['Invest']]) + unrealizedGL(plLocal[plPosition['Invest']])

	cp['SPREAD_GAIN_VAT_BS'] = 0
	cp['SPREAD_GAIN_VAT_NTV'] = 0

	cp['INTE_INCOME_BFO_TAX_BS'] = intDvd(plPosition) if plPosition['EndingQuantity'] == 0 \
		or not (isAlternative(plPosition) or isHTMBond(plPosition)) \
		else intDvd(plPosition) + unrealizedGL(plPosition)

	cp['INTE_INCOME_BFO_TAX_NTV'] = intDvd(plLocal[plPosition['Invest']]) if plPosition['EndingQuantity'] == 0 \
		or not (isAlternative(plPosition) or isHTMBond(plPosition)) \
		else intDvd(plLocal[plPosition['Invest']]) + unrealizedGL(plLocal[plPosition['Invest']])

	cp['INTE_INCOME_VAT_BS'] = 0
	cp['INTE_INCOME_VAT_NTV'] = 0
	cp['INTE_EXPNS_BS'] = 0
	cp['INTE_EXPNS_NTV'] = 0
	cp['EXCH_RATE'] = 1.0 if metaTaxlot['BookCurrency'] == plPosition['Currency'] else \
		metaTaxlot['FX'][plPosition['Currency']]
	cp['EXCH_GL'] = plPosition['UnrealizedFX'] + plPosition['RealizedFX']
	cp['OTHER_PL_BS'] = plPosition['OtherIncome']
	cp['OTHER_PL_NTV'] = plLocal[plPosition['Invest']]['OtherIncome']
	cp['PL_ADJUST_BS'] = 0
	cp['PL_ADJUST_NTV'] = 0
	cp['ENTRY_TIME'] = timestampnow()

	return cp



"""
	[List] cashLedger (positions from cash ledger report)
	[List] purchaseSales (positions from purchase sales report)
	[Dictionary] plPosition (a position from profit loss report, which is considered
				a alternative type position, e.g., fixed deposit position or
				private fund)

		=> [Dictionary] a position in ALT_BASE_INFO, ALT_EXT_INFO, ALT_PARTY_INFO
						tables

	Map an alternative position from the profit loss report to a position
	that can be written to ALT_BASE_INFO, ALT_EXT_INFO, ALT_PARTY_INFO tables. 
"""
altPosition = lambda cashLedger, purchaseSales, plPosition: \
compose(
	lambda p: mergeDictionary( p
					   		 , { 'ITEM_CD': getSecurityId(plPosition['Invest'])
					   	 	   , 'CURR_CD': plPosition['Currency']
					   	 	   , 'ENTRY_TIME': timestampnow()
					   	 	   }
					   	 	 )

  , lambda plPosition: \
		altPositionFI(cashLedger, purchaseSales, plPosition) if plPosition['PrintGroup'] == 'Fixed Deposit' \
			else altPositionEQ(plPosition)

  , lambda _1, _2, plPosition: \
  		lognContinue('altPosition(): {0}'.format(plPosition['Invest']), plPosition)
)(cashLedger, purchaseSales, plPosition)



"""
	[List] cashLedger (positions from cash ledger report)
	[List] purchaseSales (positions from purchase sales report)
	[Dictionary] plPosition (a fixed deposit position from profit loss report)
		=> [Dictionary] a position in ALT_BASE_INFO, ALT_EXT_INFO, ALT_PARTY_INFO
						tables

"""
altPositionFI = lambda cashLedger, purchaseSales, plPosition: \
compose(
	lambda p: mergeDictionary( p
				   		 	 , { 'INTE_STAR_DT': p['START_DT']
				   	 	       , 'MATURITY_DT': p['END_DT']
				   	 	   	   }
				   	 	 	 )
  , lambda t: \
  		{ 'ITEM_NM': plPosition['Invest']
  		, 'ITEM_TP_CD': '03'
  		, 'START_DT': findStartDate(plPosition['Invest'], purchaseSales)
  		, 'END_DT': findMaturityDate(plPosition['Description'], cashLedger)
  		, 'ISSUE_AMT': 0
  		, 'INVEST_AMT': 0
  		, 'ITEM_BATCH': '01'
  		, 'PARTY_TP_CD': '7'
  		, 'PARTY_ORG_ID': t[0]
  		, 'COUPON_RATE': t[1]
  		}
  , compose(	# ([String] bank code, [Float] coupon rate)
  		lambda m: (getBankCode(m.group(1)), float(m.group(2)))
  	  , lambda p: re.match('([A-Za-z]*)\s+Fixed Deposit\s+([0-9.]*)', p['Invest'])
  	)
  , lambda _1, _2, plPosition: \
  		lognContinue('altPositionFI(): {0}'.format(plPosition['Invest']), plPosition)
)(cashLedger, purchaseSales, plPosition)



"""
	[Dictionary] plPosition (a fixed deposit position from profit and loss report)
		=> [Dictionary] a position in ALT_BASE_INFO, ALT_EXT_INFO, ALT_PARTY_INFO
						tables

	For private funds only
"""
altPositionEQ = lambda plPosition: \
	mergeDictionary( { # ALT_BASE_INFO
					   'ITEM_TP_CD': ''	# FIXME: The type for private fund
			 		 , 'END_DT': '2999-12-31'
			 		 , 'MANA_ORG': '27956470'
			 		 , 'FIN_INVEST_DIRECTION': '4'
					 , 'ALT_EQUITY_TP_CD': '4'

					 # ALT_EXT_INFO
					 , 'IS_LIST_EQUITY': 0
					 , 'IS_LONG_EQUITY_ITEM': 0
					 , 'FUND_TP_CD': '1'
					 , 'FUND_EXSIT_STATUS_CD': '2'

					 # ALT_PARTY_INFO
					 , 'PARTY_TP_CD': '2'
					 , 'PARTY_ORG_ID': '27956470'
					 }

				   , AltFundInfo[plPosition['Invest']])



"""
	Hard code some fields in ALT_BASE_INFO, ALT_EXT_INFO, ALT_PARTY_INFO 
	for private funds.

	Reasons to hard code:

	(1) These fields are static for a fund;
	(2) THese fields cannot be derived with logic from the fund position.
"""
AltFundInfo = {
	'China Life Franklin First Seafront Multi-Income Fund SP': \
		{
		  'ITEM_NM': 'China Life Franklin First Seafront Multi-Income Fund SP'
		, 'START_DT': '2019-11-01'
		, 'INVEST_AMT': 3832688.50	# 20051 invested this amount at the beginning
		, 'ITEM_SITUATION_INFO': 'A private fund doing equity and bond'
		, 'ISSUE_AMT': 7662933.12	# 60,000,000 HKD invested when fund initialized (USD amount)
		}

  , 'CHLIFSE HK': \
		{
		  'ITEM_NM': 'China Life Franklin Special Event Fund'
		, 'START_DT': '2013-01-01'	# just a rough date
		, 'INVEST_AMT':	0	# not sure
		, 'ITEM_SITUATION_INFO': 'A privatea fund specializing in equity IPO'
		, 'ISSUE_AMT': 0	# not sure
		}
}



pad0 = lambda x: str(x) if x > 99 else ('0' + str(x) if x > 9 else '00' + str(x))
createItemId = lambda id_, date_: ''.join(date_.split('-')) + pad0(id_ % 1000)


"""
	[String] investId => [String] security id for the alternative investment
"""
getSecurityId = compose(
	  lambda t: createItemId(t[0], t[1])
	, lambda t: t[0] if t[0] != None else \
		lognRaise('getSecurityId(): failed to lookup: {0}'.format(t[1]))
	, lambda investId: (lookupWithInvestId(investId), investId)
)



"""
	[String] investId, [List] ps (purchase sales records since inception)
		=> [String] security id for the alternative investment
"""
getOrCreateSecurityId = compose(
	  lambda t: createItemId(t[0], t[1])
	, lambda t: t[0] if t[0] != None else addnLookup(t[1], findStartDate(t[1], t[2]))
	, lambda investId, ps: (lookupWithInvestId(investId), investId, ps)
)



"""
	[String] investId, [List] ps (purchase sales records since inception)
		=> [String] date (the date of the first record for the investId in 
							purchase sales)

	Assume: the first buy record for an investment in the purchase sales records
	indicates the inception date of that investment in the portfolio.

	While this is most likely true for all the fixed deposits since they are
	generally short term, it can be wrong for other long term alternative assets 
	such as a fund. We use this function for two purposes:

	(1) Find the start date of a fixed deposit, to show in ALT_BASE_INFO table.

	(2) Find a date for an alternative investment, including fixed deposit, so
		that we can give it a unique id. In this case, even if the date is not its
		exact start date, it does no big harm.

	For long term alternative assets, we use the AltInfo dictionary to hard code
	its start date to show in the ALT_BASE_INFO table.
"""
findStartDate = compose(
	  lambda t: t[0]['TradeDate'] if t[0] != None else \
		lognRaise('findStartDate(): failed to find start date: {0}'.format(t[1]))
	, lambda investId, ps: \
		( firstOf( lambda p: p['InvestID'] == investId and p['TranType'] == 'Buy'
				 , ps)
		, investId
		)
)



"""
	[String] investment (investment description from profit loss report), 
	[List] cl (cash ledger records since inception)
		=> [String] date (the maturity date for a fixed deposit position)
"""
findMaturityDate = compose(
	  lambda t: t[0]['CashDate'] if t[0] != None else maturityDateFromDes(t[1])
	, lambda investment, cl: \
		( firstOf( lambda p: p['Investment'] == investment and p['TranDescription'] == 'Mature'
				 , cl)
		, investment
		)
)



"""
	[String] investment => [String] date (yyyy-mm-dd)

	Work out the maturity date from the invest id of a fixed deposit.

	The investment looks like: "BOCom Fixed Deposit 2.5 23/12/2019"

	NOTE: this function assumes the date in the format of the date is
	dd/mm/yyyy, and this is only true after early December 2019. Before
	there are cases of mm/dd/yyyy.

	Sometimes the yyyy year number may be just yy. We will take care of
	that.
"""
maturityDateFromDes = compose(
	  lambda t: t[2] + '-' + t[1] + '-' + t[0]
	, lambda t: (t[0], t[1], '20' + t[2] if len(t[2]) == 2 else t[2])
	, lambda des: des.split()[-1].split('/')
)



def getLocalBookSum(investment, currency, income):
	"""
	[String] investment (investment description), 
	[String] currency (local currency of the investment),
	[Dictionary] income =>

	(dividend receivable book, dividend receivable local)

	Where income is a dictionary mapping the position to a List of tuple 
	(date, currency, local value, book value). Usually from the cash ledger
	or dividend receivable report.

	Usually the currency of a dividend is the same as the local currency of
	the stock. But there are exceptions, e.g., Ping An Insurance (H) can
	announce dividends in CNY, but the stock's local currency is HKD. In
	this rare case, we need to lookup the FX rate to convert CNY to HKD on
	the ExDividend day.

	The reason to use FX rate on ExDividend day is that:

	1) It's reasonable to do so;
	2) If we use FX rate on the reporting day, then the FX rate keeps changing,
		therefore the dividend receivable keeps changing, which is not desirable.
	"""
	if not investment in income:
		return (0, 0)

	return   sum(map(lambda t: t[3], income[investment]))\
		   , sum(map( lambda t: t[2] if t[1] == currency else \
		   						getHistoricalFX(t[0], t[1], currency)*t[2]
					, income[investment]))



"""
	[String] name pattern 
		=> [List] full path file names with that pattern under the data 
					directory

	For example, the pattern is 'tax lot', then return a list of files
	that starts with '20051 tax lot' under the data directory.
"""
getFileListWithName = compose(
	list
  , partial(map, lambda fn: join(getDataDirectory(), fn))
  , flip(filter, getFiles(getDataDirectory()))
  , lambda name: lambda fn: fn.startswith(name)
)



"""
	[String] name pattern 
		=> [List] full path file name with that pattern under the data 
					directory

	Here we make sure that there is one and only one file with that name
	pattern under the data directory.
"""
getFileWithName = lambda name: \
compose(
	lambda files: \
  		lognRaise('getFileWithName(): no file found for: {0}'.format(name)) \
  		if len(files) == 0 else \
  		lognRaise('getFileWithName(): too many files found for: {0}'.format(name)) \
  		if len(files) > 1 else files[0]

  , lambda name: getFileListWithName(name)
)(name)



"""
	[String] dt (yyyy-mm-dd, the report date) 
		=> [String] nav file name at last year end

	Find nav from last year end's NAV file, if this year is 2019, then the
	result should be: "Previous Year 20051 nav 20181231.xlsx"
"""
getLastYearEndNavFile = compose(
	getFileWithName
  , lambda dt: 'Previous Year 20051 nav '+ str(int(dt[0:4])-1) + '1231.xlsx'
)



def writeCsvWithInfo(name, dt, headers, positions, outputDirectory=getOutputDirectory()):
	round4digit = lambda x: round(x, 4) if isinstance(x, float) else x
	
	dictValues = lambda headers, p: \
		[round4digit(p.get(key, '')) for key in headers]

	file = join(outputDirectory, name + '-' + dt + '.csv')

	writeCsv( file
			, chain([headers], map(partial(dictValues, headers), positions))
			, quotechar='"'\
			, quoting=csv.QUOTE_NONNUMERIC)

	return file



"""
	[String] dt (yyyy-mm-dd), [Iterable] positions => [String] file

	Side effect: write a csv file for the positions
"""
writePositionCsv = lambda dt, positions: \
	writeCsvWithInfo(
	    'ACC_POSITION_INFO'\
	  , dt\
	  , [ 'BIZ_DT', 'ACC_CD', 'ACCTING_CLA_CD', 'CURR_CD', 'SEC_CD', 'EXCH_CD'\
	  	, 'LONG_SHORT_FLAG', 'HOLD_QTY', 'PURCH_COST_BS', 'PURCH_COST_NTV'\
	  	, 'PREM_DISC_BS', 'PREM_DISC_NTV', 'FIN_VALUAT_BS', 'FIN_VALUAT_NTV'\
	  	, 'FLOAT_PL_BS', 'FLOAT_PL_NTV', 'FAIR_VALU_CHG_PL_BS', 'FAIR_VALU_CHG_PL_NTV'\
	  	, 'ACCR_INTE_BS', 'ACCR_INTE_NTV', 'RECV_BONUS_BS', 'RECV_BONUS_NTV'\
	  	, 'INTE_PAYABLE_BS', 'INTE_PAYABLE_NTV', 'IMPA_PROV_BS', 'IMPA_PROV_NTV'\
		, 'IMPA_LOS_BS', 'IMPA_LOS_NTV', 'SPREAD_GAIN_BFO_TAX_BS', 'SPREAD_GAIN_BFO_TAX_NTV'\
		, 'SPREAD_GAIN_VAT_BS', 'SPREAD_GAIN_VAT_NTV', 'INTE_INCOME_BFO_TAX_BS'\
		, 'INTE_INCOME_BFO_TAX_NTV', 'INTE_INCOME_VAT_BS', 'INTE_INCOME_VAT_NTV'\
		, 'INTE_EXPNS_BS', 'INTE_EXPNS_NTV', 'EXCH_RATE', 'EXCH_GL', 'OTHER_PL_BS'\
		, 'OTHER_PL_NTV', 'PL_ADJUST_BS', 'PL_ADJUST_NTV', 'ENTRY_TIME'\
		]\
	  , positions)



writeTradeCsv = lambda dt, trades: \
	writeCsvWithInfo(
		'ACC_SEC_TRADE_INFO'\
	  , dt\
	  , [ 'BIZ_DT', 'ACC_CD', 'ACCTING_CLA_CD', 'EXCH_CD', 'SEC_CD', 'TX_TP_CD'\
	  	, 'TX_DIRECTION_CD', 'CURR_CD', 'TX_ID', 'EXCH_RATE', 'TX_QTY', 'TX_ACCR_INTE_BS'\
	  	, 'TX_ACCR_INTE_NTV', 'TX_FEE_BS', 'TX_FEE_NTV', 'COMMISSION_BS', 'COMMISSION_NTV'\
		, 'TX_AMT_BS', 'TX_AMT_NTV', 'ENTRY_TIME'\
		]\
	  , trades)



writeCashFlowCsv = lambda dt, cashFlow: \
	writeCsvWithInfo(
		'ACC_CAPTL_CHG_INFO'\
	  , dt\
	  , [ 'BIZ_DT', 'ACC_CD', 'TX_DIRECTION_CD', 'CURR_CD', 'EXCH_RATE'\
	  	, 'TX_AMT_BS', 'TX_AMT_NTV', 'TX_SHARE', 'ENTRY_TIME'\
	  	]\
	  , cashFlow)



writeNavCsv = lambda dt, nav: \
	writeCsvWithInfo(
		'ACC_NET_VALUE_INFO'\
	  , dt\
	  , [ 'BIZ_DT', 'ACC_CD', 'CURR_CD', 'EXCH_RATE', 'TOTAL_NAV_CNY'\
	  	, 'TOTAL_NAV_BS', 'TOTAL_SHARE', 'UNIT_NAV_CNY', 'UNIT_NAV_BS'\
	  	, 'TOT_ASSET_CNY', 'TOT_ASSET_BS', 'TOTAL_GAIN_CNY', 'TOTAL_GAIN_BS'\
		, 'ENTRY_TIME'\
	  	]\
	  , nav)



writeSecurityMapCsv = lambda dt, mapping: \
	writeCsvWithInfo(
		'SEC_MAP_INFO'\
	  , dt\
	  , [ 'INNER_SEC_CD', 'SEC_NM', 'ISIN', 'SEDOL', 'CUSIP', 'SEC_TX_CD'\
	  	, 'SEC_EXCH_CD', 'AIM_INNER_SEC_CD', 'ENTRY_TIME'
		]\
	  , mapping)



writeAltBaseInfo = lambda dt, baseInfo: \
	writeCsvWithInfo(
		'ALT_BASE_INFO'\
	  , dt\
	  , [ 'ITEM_CD', 'ITEM_NM', 'ITEM_TP_CD', 'CURR_CD', 'START_DT', 'END_DT'\
	  	, 'SW_IND2_CD', 'GICS_IND_CD', 'INVEST_OBJECT', 'INVEST_AMT', 'PERIOD'\
	  	, 'PERIOD_MEMO', 'ITEM_SITUATION_INFO', 'ITEM_MANAGER', 'MANA_ORG'\
	  	, 'ISSUE_AMT', 'FIN_INVEST_DIRECTION', 'ALT_EQUITY_TP_CD', 'REAL_ESTATE_TP_CD'\
	  	, 'CNTRY_ID', 'CNTRY_NM', 'PROV_ID', 'PROV_NM', 'PROV_INFO_MEMO'\
		, 'ITEM_INNER_RATING', 'ITEM_OUTER_RATING', 'ENTRY_TIME'\
		]\
	  , baseInfo)



writeAltExtInfo = lambda dt, extInfo: \
	writeCsvWithInfo(
		'ALT_EXT_INFO'\
	  , dt\
	  , [ 'ITEM_CD', 'ITEM_BATCH', 'INTE_STAR_DT', 'MATURITY_DT', 'COUPON_RATE'\
	  	, 'RATE_TP_CD', 'PAY_INTE_FREQ_CD', 'ACCR_INTE_RULE_TP_CD', 'ACCR_INTE_BASE_CD'\
	  	, 'BASIC_MARGIN', 'RATE_CLAUSE', 'RATE_MIN', 'RATE_MAX', 'INVEST_EXPECT_YIELD_RATE'\
		, 'CURSTAGE_EXPECT_YIELD_RATE', 'REALZ_YIELD', 'QTY_RATIO', 'TOT_SHARE'\
		, 'IS_LIST_EQUITY', 'IS_LONG_EQUITY_ITEM', 'CONTRACT_AMT', 'DIVD_CLAUSE'\
		, 'QUIT_MODE', 'FUND_TP_CD', 'FUND_EXSIT_STATUS_CD', 'IS_FUND_BASE'\
		, 'THRESHOLD_YIELD_RATE', 'ENTRY_TIME'\
		]\
	  , extInfo)



writeAltPartyInfo = lambda dt, partyInfo: \
	writeCsvWithInfo(
		'ALT_PARTY_INFO'\
	  , dt\
	  , [ 'ITEM_CD', 'PARTY_TP_CD', 'PARTY_ORG_ID', 'INNER_RATING', 'OUTER_RATING'\
	  	, 'SW_IND2_CD', 'GICS_IND_CD', 'TX_CNTPTY_CONTROLLER', 'ENTRY_TIME'\
	  	]\
	  , partyInfo
	)



"""
	[String] reportDate (yyyy-mm-dd),
	[Iterable] positions (for ACC_POSITION_INFO table)
	[List] altPositions (for ALT_BASE_INFO, ALT_EXT_INFO, ALT_PARTY_INFO tables)
	[Iterable] cashFlow (for ACC_CAPTL_CHG_INFO table)
	[Iterable] trades (for ACC_SEC_TRADE_INFO table)
	[Dictionary] one row for ACC_NET_VALUE_INFO table
	[Iterable] securityMapping (for SEC_MAP_INFO table)
		=> (tuple for output csv file names)
"""
outputCsv = lambda reportDate, positions, altPositions, cashFlow \
				, trades, navRow, securityMapping: \
	( writePositionCsv(reportDate, positions)
	, writeAltBaseInfo(reportDate, altPositions)
	, writeAltExtInfo(reportDate, altPositions)
	, writeAltPartyInfo(reportDate, addAltPartyInfoFund(altPositions))
	, writeCashFlowCsv(reportDate, cashFlow)\
	, writeTradeCsv(reportDate, trades)\
	, writeNavCsv(reportDate, [navRow])\
	, writeSecurityMapCsv(reportDate, securityMapping)
	)




if __name__ == '__main__':
	import logging.config
	logging.config.fileConfig('logging.config', disable_existing_loggers=False)

	"""
	Generate the below reports and put into the data directory (see config file):

	1) Tax lot appraisal report;
	2) Dividend receivable payable report;
	3) Statement of net assets report;
	4) Due to from broker report;
	5) Cash ledger report;
	6) Purchase sales report;
	7) Profit and loss reports in different base currencies.

	All the above reports should have the same period end date. For cash ledger
	and purchase sales reports, their period start date should be 1950-01-01 (the
	Geneva default). For profit and loss reports, their start date should be
	the first day of the year, say 2020-01-01 the period end date is some day
	within year 2020.

	After the reports are put into data directory, do:

	$ python handler.py

	"""
	print(outputCsv(
			*processReport( getFileWithName('20051 tax lot')\
						  , getFileWithName('20051 cash ledger')\
						  , getFileWithName('20051 dividend receivable')\
						  , getFileListWithName('20051 profit loss')\
						  , getFileWithName('20051 purchase sales')\
						  , getFileWithName('20051 due to from')\
						  , getFileWithName('20051 nav'))))