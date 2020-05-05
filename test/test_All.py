# coding=utf-8
# 

import unittest2
from xlrd import open_workbook
from trustee_report.main import readFile
from trustee_report.utility import getCurrentDirectory
from toolz.functoolz import compose
from functools import partial
from os.path import join



"""
    [Function] filterFunc (a function that takes in a position returns T/F)
    [Iterable] positions
        => number of elements in the positions that evaluates to True by
            the filterFunc 
"""
countPositions = lambda filterFunc, positions: compose(
    len
  , list
  , filter
)(filterFunc, positions)



class TestAll(unittest2.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestAll, self).__init__(*args, **kwargs)



    def testFile1(self):
        inputFile = join(getCurrentDirectory(), 'samples', '01 cash only.xls')
        positions = list(readFile(inputFile))
        self.assertEqual(4, len(positions))

        cashPositions = list(filter( lambda p: p['AssetType'] == 'Cash'
                                   , positions))
        self.assertEqual(3, len(cashPositions))
        self.verifyCashPosition(cashPositions[0])



    def testFile2(self):
        inputFile = join(getCurrentDirectory(), 'samples', '03 cash equity.xls')
        positions = list(readFile(inputFile))
        self.assertEqual(4, len(positions))

        equityPositions = list(filter( lambda p: p['AssetType'] == 'Equity'
                                     , positions))
        self.assertEqual(1, len(equityPositions))
        self.verifyEquityPosition(equityPositions[0])



    def testFile3(self):
        inputFile = join(getCurrentDirectory(), 'samples', '06 multiple cash multiple bond.xls')
        positions = list(readFile(inputFile))

        cashPositions = list(filter( lambda p: p['AssetType'] == 'Cash'
                                   , positions))
        self.assertEqual(3, len(cashPositions))

        cnyHTMBonds = list(filter( lambda p: p['AssetType'] == 'HTMBond' and \
                                                p['Currency'] == 'CNY'
                                 , positions))
        self.assertEqual(1, len(cnyHTMBonds))
        self.verifyCNYHTMBondPosition(cnyHTMBonds[0])
        
        usdAFSBonds = compose(
            list
          , partial( filter
                   , lambda p: p['AssetType'] == 'AFSBond' and p['Currency'] == 'USD')
        )(positions)

        self.assertEqual(4, len(usdAFSBonds))
        self.verifyUSDAFSBondPosition(usdAFSBonds[3])

        self.assertEqual( 67
                        , countPositions(
                                lambda p: p['AssetType'] == 'HTMBond' and p['Currency'] == 'USD'
                              , positions
                          )
                        )



    def testFile4(self):
        inputFile = join(getCurrentDirectory(), 'samples', '05 cash multiple bond.xls')
        positions = list(readFile(inputFile))
        self.assertEqual(2, countPositions(lambda p: p['AssetType'] == 'Cash', positions))
        self.assertEqual(4, countPositions(
                                lambda p: p['AssetType'] == 'AFSBond' and p['Currency'] == 'USD'
                              , positions))

        usdHTMBonds = list(filter( lambda p: p['AssetType'] == 'HTMBond' and p['Currency'] == 'USD'
                                 , positions))
        self.assertEqual(51, len(usdHTMBonds))
        self.verifyUSDHTMBondPosition(usdHTMBonds[0])



    def verifyCashPosition(self, p):
        self.assertEqual('USD', p['Currency'])
        self.assertEqual('12341', p['Portfolio'])
        self.assertAlmostEqual(-9136.08, p['Cost'])
        self.assertAlmostEqual(-9136.08, p['MarketValue'])



    def verifyEquityPosition(self, p):
        self.assertEqual('HKD', p['Currency'])
        self.assertEqual('12298', p['Portfolio'])
        self.assertEqual('00823.HK The Link Real Estate', p['Description'])
        self.assertEqual(2160000, p['Quantity'])
        self.assertAlmostEqual(199951856.44, p['Cost'])
        self.assertAlmostEqual(155952000, p['MarketValue'])
        self.assertAlmostEqual(72.2, p['MarketPrice'])



    def verifyCNYHTMBondPosition(self, p):
        self.assertEqual('12229', p['Portfolio'])
        self.assertEqual('HK0000171949 Beijing Enterprises', p['Description'])
        self.assertEqual(200000000, p['Quantity'])
        self.assertAlmostEqual(200000000, p['Cost'])
        self.assertAlmostEqual(100, p['AmortizedCost'])



    def verifyUSDAFSBondPosition(self, p):
        self.assertEqual('12229', p['Portfolio'])
        self.assertEqual('XS2125922349 BANK OF CHINA 3.6%', p['Description'])
        self.assertEqual(40000000, p['Quantity'])
        self.assertAlmostEqual(39910000, p['Cost'])
        self.assertAlmostEqual(98.706, p['MarketPrice'])



    def verifyUSDHTMBondPosition(self, p):
        self.assertEqual('12366', p['Portfolio'])
        self.assertEqual('FR0013101599 CNP ASSURANCES', p['Description'])
        self.assertEqual(494400000, p['Quantity'])
        self.assertAlmostEqual(559057632, p['Cost'])
        self.assertAlmostEqual(112.738861385518, p['AmortizedCost'])