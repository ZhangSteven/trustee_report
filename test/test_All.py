# coding=utf-8
# 

import unittest2
from xlrd import open_workbook
from os.path import join



class TestAll(unittest2.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestAll, self).__init__(*args, **kwargs)



    def test0(self):
    	self.assertEqual(0, 0)