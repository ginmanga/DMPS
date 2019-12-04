import datetime, csv
#from sas7bdat import SAS7BDAT
import pandas as pd
import pandasql as ps
import numpy as np
import gzip, os, csv

#FUNDAMAINID, CRSPLINK, CAPIQID, FUNDADEBT, FUNDACMP, SPCR = LoadData.load_data_main()


#Merge FUNDA WITH CRSPLINK BUT WITH DUPLICATES
#COLLAPSE THE TABLE AND REMERGE PROPERLY


CRSPLINK = pd.read_csv(os.path.join(datadirectory, "collapsedlink.csv"), index_col=0)

COMPUCRSP = pd.merge(FUNDAMAINID,
                         CRSPLINK[['gvkey','conm','LPERMNO','LPERMCO','cusip','cik','LINKDT','LINKENDDT', 'sic', 'ipodate']],
                         left_on=['gvkey'],
                         right_on = ['gvkey'], how='left')

#ERASE DUPLICATES AND MISSING
#COMPUCRSP.loc[COMPUCRSP.LINKENDDT == 'E', 'LINKENDDT'] = 20191012
COMPUCRSP = COMPUCRSP.dropna(subset=['LINKENDDT'])
COMPUCRSP = COMPUCRSP.astype({'datadate': 'int64', 'LINKDT':'int64', 'LINKENDDT':'int64'})
COMPUCRSP = COMPUCRSP[(COMPUCRSP.datadate >= COMPUCRSP.LINKDT) & (COMPUCRSP.datadate <= COMPUCRSP.LINKENDDT)]

#COMPUCRSP_T = COMPUCRSP[(COMPUCRSP.sic >= 7000) & (COMPUCRSP.sic < 6000)].index

#df = df[df['closing_price'].between(99, 101)]

#f = list(range(6000, 6999))
#COMPUCRSP['fin'] = [1 if x in f else 0 for x in COMPUCRSP['sic']]
#COMPUCRSP = COMPUCRSP[COMPUCRSP.fin == 0]

#f2 = list(range(4900, 4949))
#COMPUCRSP['util'] = [1 if x in f2 else 0 for x in COMPUCRSP['sic']]
#COMPUCRSP = COMPUCRSP[COMPUCRSP.util == 0]
#COMPUCRSP = COMPUCRSP.drop(['fin','util'], axis = 1)
#erase utilities
#MERGE CAPIIQ

###MERGED####
#### NEED TO FIX THIS ##########
COMPUCRSPIQ = pd.merge(COMPUCRSP,
                         CAPIQID[['gvkey','companyid','startdate', 'enddate']],
                         left_on=['gvkey'],
                         right_on = ['gvkey'], how='left')

datadirectory = os.path.join(os.getcwd(), 'data')
#what does this do?
COMPUCRSPIQ.loc[COMPUCRSPIQ.enddate == 'E', 'enddate'] = 20191012
COMPUCRSPIQ.loc[COMPUCRSPIQ.startdate == 'B', 'startdate'] = 19680101
COMPUCRSPIQ['startdate'].fillna(19700101, inplace=True)
COMPUCRSPIQ['enddate'].fillna(20191012, inplace=True)
COMPUCRSPIQ = COMPUCRSPIQ.astype({'datadate': 'int64', 'startdate':'int64', 'enddate':'int64'})
COMPUCRSPIQ = COMPUCRSPIQ[(COMPUCRSPIQ.datadate >= COMPUCRSPIQ.startdate) & (COMPUCRSPIQ.datadate <= COMPUCRSPIQ.enddate)]


#Write to file: STARTING POINT NEXT STEPS

COMPUCRSPIQ.to_csv(os.path.join(datadirectory, "MERGEDI-DOCT19.csv"))
COMPUCRSPIQ = pd.read_csv(os.path.join(datadirectory, "MERGEDI-DOCT19.csv"), index_col=0)


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
SPCRNONA['tempdays'] = (SPCRNONA['datadate'] - SPCRNONA['temp']).dt.days
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

COMPUCRSPIQCR = COMPUCRSPIQCR.drop(['startdate','enddate'], axis = 1)


COMPUCRSPIQCR.to_csv(os.path.join(datadirectory, "MERGEDIDCR-ALLNOV27.csv"))



COMPUCRSPIQ_small = COMPUCRSPIQCR[['gvkey','datadate','LPERMNO']]



#Make new variable using SICH, SIC from compustat and SICCD from CRSP
FUNDASIC = pd.read_csv(os.path.join(datadirectory, "FUNDASICH.gz"), sep='\t') #funda with SICH and SIC
COMPUCRSPIQCR = pd.read_csv(os.path.join(datadirectory, "MERGEDIDCR-ALLNOV27.csv"), index_col=0) #merged CRSPCOMPUCAPIQ
FUNDALIST_CRSPIDS = pd.read_csv(os.path.join(datadirectory, "FUNDALIST_CRSPIDSNOV27.csv"), index_col=0) #merged CRSPIDS

COMPUCRSPIQCR['datadate'] = pd.to_datetime(COMPUCRSPIQCR['datadate'])
FUNDALIST_CRSPIDS['datadate'] = pd.to_datetime(FUNDALIST_CRSPIDS['datadate'])

#USE SICH when avaialable

#Collapse FUNDASIC by sich


#merge SICH back into FUNDASIC
#prepare matching ID for us with asof
FUNDASIC['temp'] = '19600101'
FUNDASIC['temp'] = pd.to_datetime(FUNDASIC['temp'])
FUNDASIC['datadate'] = pd.to_datetime(FUNDASIC['datadate'], format='%Y%m%d')
FUNDASIC['tempdays'] = (FUNDASIC['datadate']-FUNDASIC['temp']).dt.days
FUNDASIC['gvkey'] = FUNDASIC['gvkey'].apply(int)
FUNDASIC['gvkey'] = FUNDASIC['gvkey'].apply(str)
FUNDASIC['temp'] = '10000000'
FUNDASIC['tempID'] = FUNDASIC['gvkey'] + FUNDASIC['temp']
FUNDASIC['tempID'] = FUNDASIC['tempID'].apply(int)
FUNDASIC['tempID'] = FUNDASIC['tempID'] + FUNDASIC['tempdays']

#same for FUNDAQ
FUNDAQ['temp'] = '19600101'
FUNDAQ['temp'] = pd.to_datetime(FUNDAQ['temp'])
FUNDAQ['datadate'] = pd.to_datetime(FUNDAQ['datadate'], format='%Y%m%d')
FUNDAQ['tempdays'] = (FUNDAQ['datadate']-FUNDAQ['temp']).dt.days
FUNDAQ['gvkey'] = FUNDAQ['gvkey'].apply(int)
FUNDAQ['gvkey'] = FUNDAQ['gvkey'].apply(str)
FUNDAQ['temp'] = '10000000'
FUNDAQ['tempID'] = FUNDAQ['gvkey'] + FUNDAQ['temp']
FUNDAQ['tempID'] = FUNDAQ['tempID'].apply(int)
FUNDAQ['tempID'] = FUNDAQ['tempID'] + FUNDAQ['tempdays']
#drop nans
FUNDASIC_nonan = FUNDASIC.dropna(subset=['sich'])
FUNDASIC_NEWSIC = pd.merge_asof(FUNDASIC, FUNDASIC_nonan[['gvkey','sich','tempID']], on='tempID', direction='nearest')
# now turn sich_y into sic if gkvey_x not equal gvkey_y
FUNDASIC_NEWSIC['sic_ch'] = np.where(FUNDASIC_NEWSIC['gvkey_x'] == FUNDASIC_NEWSIC['gvkey_y'],
                                     FUNDASIC_NEWSIC['sich_y'], FUNDASIC_NEWSIC['sic'])

FUNDASIC_NEWSIC.rename(columns={'gvkey_x': 'gvkey'}, inplace=True)

#Merge new SIC to FUNDALIST_CRSPIDS create SIC first compustat then crsp, then establish FF48industry
FUNDASIC_NEWSIC['gvkey'] = FUNDASIC['gvkey'].apply(int)
FUNDALIST_CRSPIDS = pd.merge(FUNDALIST_CRSPIDS,
                         FUNDASIC_NEWSIC[['gvkey','datadate','sic_ch']],
                         left_on=['gvkey','datadate'],
                         right_on = ['gvkey','datadate'], how='left')

COMPUCRSPIQCR = pd.merge(COMPUCRSPIQCR,
                         FUNDASIC_NEWSIC[['gvkey','datadate','sic_ch']],
                         left_on=['gvkey','datadate'],
                         right_on = ['gvkey','datadate'], how='left')


FUNDAQ = pd.merge_asof(FUNDAQ, FUNDASIC_NEWSIC[['gvkey','sic_ch','tempID']], on='tempID', direction='nearest')
FUNDAQ.rename(columns={'gvkey_x': 'gvkey'}, inplace=True)
#check match
FUNDAQ = FUNDAQ.astype({'gvkey_y': 'str'})
FUNDAQ['check'] = np.where(FUNDAQ['gvkey'] == FUNDAQ['gvkey_y'], 1, 0)
FUNDAQ[FUNDAQ.check==0]

FUNDAQ_small = FUNDAQ[FUNDAQ.check==0]
#delete unnecessary columns
to_del_fundasic = ['sich_x','temp','tempdays','tempID','gvkey_y','sich_y','sicNEW']
to_del_fundaq = ['temp','tempdays','tempID','gvkey_y']

FUNDASIC_NEWSIC = FUNDASIC_NEWSIC.drop(to_del_fundasic, axis=1)
FUNDAQ = FUNDAQ.drop(to_del_fundaq, axis=1)
### Now get Fama french industry for sic_ch
FUNDASIC_NEWSIC['sic_ch'] = FUNDASIC_NEWSIC['sic_ch'].apply(lambda x: str(x).zfill(4))
FUNDALIST_CRSPIDS['sic_ch'] = FUNDALIST_CRSPIDS['sic_ch'].apply(lambda x: str(x).zfill(4))
COMPUCRSPIQCR['sic_ch'] = COMPUCRSPIQCR['sic_ch'].apply(lambda x: str(x).zfill(4))
FUNDAQ['sic_ch'] = FUNDAQ['sic_ch'].apply(lambda x: str(x).zfill(4))
ff48_dict = json.load(open(os.path.join(datadirectory, 'FF48.txt')))

FUNDASIC_NEWSIC['FF48'] = FUNDASIC_NEWSIC['sic_ch'].apply(lambda x: ff48_dict[x])
FUNDALIST_CRSPIDS['FF48'] = FUNDALIST_CRSPIDS['sic_ch'].apply(lambda x: ff48_dict[x])
COMPUCRSPIQCR['FF48'] = COMPUCRSPIQCR['sic_ch'].apply(lambda x: ff48_dict[x])
FUNDAQ['FF48'] = FUNDAQ['sic_ch'].apply(lambda x: ff48_dict[x])

#SAVEALL
COMPUCRSPIQCR.to_csv(os.path.join(datadirectory, "MERGEDIDCR-ALLDEC4.csv"))
FUNDALIST_CRSPIDS.to_csv(os.path.join(datadirectory, "FUNDALIST_CRSPIDSDEC4.csv"))
FUNDASIC_NEWSIC.to_csv(os.path.join(datadirectory, "FUNDASIC_DEC4.csv"))
FUNDAQ.to_csv(os.path.join(datadirectory, "FUNDAQDEC4.csv"))

#Make new variable using SICH, SIC from compustat and SICCD from CRSP
FUNDASIC = pd.read_csv(os.path.join(datadirectory, "FUNDASICH.gz"), sep='\t') #funda with SICH and SIC


#make dictionary with fama-fench industry and set of all industries (function created to make dictionaries)

#match quarterly to fundasec_newsic

ff48_dict = json.load(open(os.path.join(datadirectory, 'FF48.txt')))

ff48_dictt['4970']

ff = Functions.fama_french_ind(datadirectory, "Siccodes48.txt", nametosave='FF48.txt', option=1)
ff['9996']
ff['3990']