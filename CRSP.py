# Add actual company names to link table
# CRSP SIC numbers and other necessary things
# get gvkey from link table
#then merge crsp info to
COMPUCRSPIQ_small = pd.read_csv(os.path.join(datadirectory, "MERGEDIDCR-ALLNOV27.csv"), index_col=0)
COMPUCRSPIQ_small['datadate'] = pd.to_datetime(COMPUCRSPIQ_small['datadate'])

#FUNDALIST_CRSPIDS = pd.merge(COMPUCRSPIQ_small,
                   # CRSPM[['PERMNO','date','SHRCD', 'EXCHCD', 'SICCD','TICKER', 'COMNAM',
                           #'CUSIP','NCUSIP']],
                    #left_on=['LPERMNO','datadate'],
                    #right_on = ['PERMNO','date'], how='left')

#C = CRSPM.dropna(subset=['SHRCLS'])

CRSPM =  CRSPM[['PERMNO','date','SHRCD', 'EXCHCD', 'SICCD','TICKER', 'COMNAM', 'CUSIP','NCUSIP']]

COMPUCRSPIQ_small = COMPUCRSPIQ_small.sort_values(by=['LPERMNO', 'datadate'])
CRSPM = CRSPM.sort_values(by=['PERMNO', 'date'])

###Building Key to match
COMPUCRSPIQ_small['temp'] = '19600101'
CRSPM['temp'] = '19600101'
COMPUCRSPIQ_small['temp'] = pd.to_datetime(COMPUCRSPIQ_small['temp'])
CRSPM['temp'] = pd.to_datetime(CRSPM['temp'])
COMPUCRSPIQ_small['tempdays'] = (COMPUCRSPIQ_small['datadate']-COMPUCRSPIQ_small['temp']).dt.days
CRSPM['tempdays'] = (CRSPM['date'] - CRSPM['temp']).dt.days


COMPUCRSPIQ_small['LPERMNO'] = COMPUCRSPIQ_small['LPERMNO'].apply(int)
COMPUCRSPIQ_small['LPERMNO'] = COMPUCRSPIQ_small['LPERMNO'].apply(str)
CRSPM['PERMNO'] = CRSPM['PERMNO'].apply(str)
COMPUCRSPIQ_small['temp'] = '10000000'
CRSPM['temp'] = '10000000'
COMPUCRSPIQ_small['tempID'] = COMPUCRSPIQ_small['LPERMNO'] + COMPUCRSPIQ_small['temp']
CRSPM['tempID'] = CRSPM['PERMNO'] + CRSPM['temp']
COMPUCRSPIQ_small['tempID'] = COMPUCRSPIQ_small['tempID'].apply(int)
CRSPM['tempID'] = CRSPM['tempID'].apply(int)
COMPUCRSPIQ_small['tempID'] = COMPUCRSPIQ_small['tempID'] + COMPUCRSPIQ_small['tempdays']
CRSPM['tempID'] = CRSPM['tempID'] + CRSPM['tempdays']


COMPUCRSPIQ_small = COMPUCRSPIQ_small.sort_values(by=['tempID'])
CRSPM = CRSPM.sort_values(by=['tempID'])


FUNDALIST_CRSPIDS = pd.merge_asof(COMPUCRSPIQ_small, CRSPM, on='tempID')
####### Drop extra ones
todrop = ['conm_y', 'cusip_y', 'cik_y','temp_x','tempdays_x','tempID','temp_y','tempdays_y']

FUNDALIST_CRSPIDS = FUNDALIST_CRSPIDS.drop(todrop, axis=1)
FUNDALIST_CRSPIDS.to_csv(os.path.join(datadirectory, "FUNDALIST_CRSPIDSNOV27.csv"))