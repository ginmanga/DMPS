import datetime, csv
#from sas7bdat import SAS7BDAT
import pandas as pd
import pandasql as ps
import numpy as np
import gzip, os, csv

#FUNDAMAINID, CRSPLINK, CAPIQID, FUNDADEBT, FUNDACMP, SPCR = LoadData.load_data_main()


#Merge FUNDA WITH CRSPLINK BUT WITH DUPLICATES
#COLLAPSE THE TABLE AND REMERGE PROPERLY
COMPUCRSP = pd.merge(FUNDAMAINID,
                         CRSPLINK[['gvkey','conm','LPERMNO','LPERMCO','cusip','cik','LINKDT','LINKENDDT', 'sic']],
                         left_on=['gvkey'],
                         right_on = ['gvkey'], how='left')

#ERASE DUPLICATES AND MISSING
COMPUCRSP.loc[COMPUCRSP.LINKENDDT == 'E', 'LINKENDDT'] = 20191012
COMPUCRSP = COMPUCRSP.dropna(subset=['LINKENDDT'])
COMPUCRSP = COMPUCRSP.astype({'datadate': 'int64', 'LINKDT':'int64', 'LINKENDDT':'int64'})
COMPUCRSP = COMPUCRSP[(COMPUCRSP.datadate >= COMPUCRSP.LINKDT) & (COMPUCRSP.datadate <= COMPUCRSP.LINKENDDT)]

COMPUCRSP_T = COMPUCRSP[(COMPUCRSP.sic >= 7000) & (COMPUCRSP.sic < 6000)].index

#df = df[df['closing_price'].between(99, 101)]

f = list(range(6000, 6999))
COMPUCRSP['fin'] = [1 if x in f else 0 for x in COMPUCRSP['sic']]
COMPUCRSP = COMPUCRSP[COMPUCRSP.fin == 0]

f2 = list(range(4900, 4949))

COMPUCRSP['util'] = [1 if x in f else 0 for x in COMPUCRSP['sic']]
COMPUCRSP = COMPUCRSP[COMPUCRSP.util == 0]

#erase utilities
#MERGE CAPIIQ

###MERGED####
#### NEED TO FIX THIS ##########
COMPUCRSPIQ = pd.merge(COMPUCRSP,
                         CAPIQID[['gvkey','companyid','startdate', 'enddate']],
                         left_on=['gvkey'],
                         right_on = ['gvkey'], how='left')

datadirectory = os.path.join(os.getcwd(), 'data')

COMPUCRSPIQ.loc[COMPUCRSPIQ.enddate == 'E', 'enddate'] = 20191012
COMPUCRSPIQ.loc[COMPUCRSPIQ.startdate == 'B', 'startdate'] = 19680101
COMPUCRSPIQ['startdate'].fillna(19700101, inplace=True)
COMPUCRSPIQ['enddate'].fillna(20191012, inplace=True)
COMPUCRSPIQ = COMPUCRSPIQ.astype({'datadate': 'int64', 'startdate':'int64', 'enddate':'int64'})
COMPUCRSPIQ = COMPUCRSPIQ[(COMPUCRSPIQ.datadate >= COMPUCRSPIQ.startdate) & (COMPUCRSPIQ.datadate <= COMPUCRSPIQ.enddate)]


#Write to file: STARTING POINT NEXT STEPS

COMPUCRSPIQ.to_csv(os.path.join(datadirectory, "MERGEDID.csv"))

COMPUCRSPIQ = pd.read_csv(os.path.join(datadirectory, "MERGEDID.csv"), index_col=0)

SPCRNONA = LoadData.load_data_main_2()

COMPUCRSPIQ_small = COMPUCRSPIQ[['gvkey','datadate']]
COMPUCRSPIQ_small = COMPUCRSPIQ_small[COMPUCRSPIQ_small.datadate <= 20181231]



#Match gvkey datadate to SP credit rating file
#First convert datadates to dates and make a variable with difference to 19601010

COMPUCRSPIQ_small['datadate'] = pd.to_datetime(COMPUCRSPIQ_small['datadate'], format='%Y%m%d')
SPCRNONA['datadate'] = pd.to_datetime(SPCRNONA['datadate'], format='%Y%m%d')

#Convert dates to days because I can't find a better way...
COMPUCRSPIQ_small['temp'] = '19600101'
SPCRNONA['temp'] = '19600101'
COMPUCRSPIQ_small['temp'] = pd.to_datetime(COMPUCRSPIQ_small['temp'])
SPCRNONA['temp'] = pd.to_datetime(SPCRNONA['temp'])
COMPUCRSPIQ_small['tempdays'] = (COMPUCRSPIQ_small['datadate']-COMPUCRSPIQ_small['temp']).dt.days
SPCRNONA['tempdays'] = (SPCRNONA['datadate']-SPCRNONA['temp']).dt.days
# last date on SP CR data base 20170228
#Merge

sqlcode = '''
select a.gvkey, a.datadate, b.splticrm, b.spsdrm, b.spsticrm,
a.tempdays - b.tempdays as DLR
from COMPUCRSPIQ_small as a 
left join SPCRNONA as b on a.gvkey = b.gvkey
where ((DLR >= 0) and DLR <= 671)
'''

COMPUCRSPIQCR = ps.sqldf(sqlcode, globals())
#COMPUCRSPIQCR = ps.sqldf(sqlcode, locals())

COMPUCRSPIQCR= COMPUCRSPIQCR.sort_values(by=['gvkey', 'datadate', 'DLR'])
COMPUCRSPIQCR1 = COMPUCRSPIQCR.drop_duplicates(subset=['gvkey', 'datadate'])

COMPUCRSPIQ['datadate'] = pd.to_datetime(COMPUCRSPIQ['datadate'], format='%Y%m%d')
COMPUCRSPIQCR1['datadate'] = pd.to_datetime(COMPUCRSPIQCR1['datadate'])
COMPUCRSPIQCR1 = COMPUCRSPIQCR1.astype({'gvkey': 'int64'})

#MERGE BACK TO MAIN
COMPUCRSPIQCR = pd.merge(COMPUCRSPIQ,
                         COMPUCRSPIQCR1[['gvkey','datadate','splticrm','spsdrm','spsticrm','DLR']],
                         left_on=['gvkey','datadate'],
                         right_on = ['gvkey','datadate'], how='left')

#Write to file: STARTING POINT NEXT STEPS
#This data set includes:
# gvkey, cusip, cik, permno, permco, LINKDT, LINKENDDT, COMPANYID (CAPIQ ID),
#splticrm, spsdrm, spsticrm, DLR (days since last rating)

COMPUCRSPIQCR = COMPUCRSPIQCR.drop(['startdate','enddate','fin','util'], axis = 1)


COMPUCRSPIQCR.to_csv(os.path.join(datadirectory, "MERGEDIDCR.csv"))



COMPUCRSPIQ_small = COMPUCRSPIQCR[['gvkey','datadate','splticrm','sic']]
