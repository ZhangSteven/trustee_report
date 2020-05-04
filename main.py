# coding=utf-8
#
# Read China Life Trustee monthly reports (Excel format), convert to upload format
# for HTM price uploading to Bloomberg AIM.
#
# For input and output file directories, check the config file.
# 

from xlrd import open_workbook
import logging
logger = logging.getLogger(__name__)



def lognRaise(msg):
	logger.error(msg)
	raise ValueError


def lognContinue(msg, x):
	logger.debug(msg)
	return x




if __name__ == '__main__':
	import logging.config
	logging.config.fileConfig('logging.config', disable_existing_loggers=False)

	logger.debug('Hello')