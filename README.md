# trustee_report
Convert China Life Trustee monthly statements (Excel) to Bloomberg AIM TSCF upload file (csv format).



++++++++
Caution
++++++++

1. The program encounters an encoding related error when running on the home laptop, but runs OK on office PC. Guess this is related to the Chinese characters in the Excel, office PC has a setting that treat all non-unicode characters as Simplified Chinese, but laptop does not have that setting. So what to do?

2. Some of the trustee HTM bond holdings don't use ISIN codes, we need to find them out and map those to ISIN codes. Maybe can try pull out all HTM holdings, try loading information using those identifiers, if fail then it is not ISIN.



