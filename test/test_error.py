# coding=utf-8
# 

import unittest2
from xlrd import open_workbook
from trustee_report.main import readFile
from trustee_report.utility import getCurrentDirectory
from os.path import join



class TestError(unittest2.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestError, self).__init__(*args, **kwargs)



    def testFile1(self):
        inputFile = join( getCurrentDirectory(), 'samples', 'wrong fund name.xls')
        try:
            readFile(inputFile)
        except:
            pass    # expected: unsupported fund name
        else:
            self.fail('Error should occur, but didn\'t')



    def testFile2(self):
        inputFile = join( getCurrentDirectory(), 'samples', 'missing fund name.xls')
        try:
            readFile(inputFile)
        except:
            pass    # expected: fund name not found
        else:
            self.fail('Error should occur, but didn\'t')