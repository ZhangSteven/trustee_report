# trustee_report
Convert China Life Trustee monthly statements (Excel) to Bloomberg AIM TSCF upload file (csv format).



++++++++
Caution
++++++++

1. Some of the trustee HTM bond holdings don't use ISIN codes, we need to find them out and map those to ISIN codes. Maybe can try pull out all HTM holdings, try loading information using those identifiers, if fail then it is not ISIN.

