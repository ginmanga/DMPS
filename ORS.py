####Compile Execucomp Data nad add to main analysis file
import os
import pandas as pd
import numpy as np
#COMPUCRSPIQCR = pd.read_csv(os.path.join(datadirectory, "FUNDALIST_March29.csv.gz"))
COMPUCRSPIQCR = pd.read_csv(os.path.join(datadirectory, "FUNDALIST_MARCH03.csv.gz"))

COMPUCRSPIQCR['CNEW6D'] = COMPUCRSPIQCR['NCUSIP'].astype(str)
COMPUCRSPIQCR['CNEW6D'] = COMPUCRSPIQCR['CNEW6D'].apply(lambda x: str(x).split('.')[0])
COMPUCRSPIQCR['CNEW6D'] = COMPUCRSPIQCR['CNEW6D'].apply(lambda x: str(x).zfill(8))
COMPUCRSPIQCR['CNEW6D'] = COMPUCRSPIQCR['CNEW6D'].str.slice(0, 6)
#COMPUCRSPIQCR['CNEW6D'] = COMPUCRSPIQCR['CNEW6D'].apply(lambda x: str(x).zfill(6))

COMPUCRSPIQCRS = COMPUCRSPIQCR[['gvkey', 'PERMNO', 'NCUSIP',
                                'datadate', 'fyear', 'COMNAM']]
COMPUCRSPIQCR = COMPUCRSPIQCR[['gvkey', 'datadate', 'fyear', 'PERMNO',
                               'NCUSIP', 'CNEW6D', 'COMNAM']]
COMPUCRSPIQCR = COMPUCRSPIQCR.drop_duplicates(subset=['CNEW6D'])
COMPUCRSPIQCR = COMPUCRSPIQCR[['gvkey', 'PERMNO', 'NCUSIP', 'CNEW6D']]


COMPUCRSPIQCRS['datadate'] = pd.to_datetime(COMPUCRSPIQCRS['datadate'])

#COMPUCRSPIQCR['CNEW6D'] = COMPUCRSPIQCR['CNEW6D'].astype(int)
#B=COMPUCRSPIQCR.dropna(subset=['COMNAM'])
#a=B[B['COMNAM'].str.contains("WANG")]
#a['CNEW6D'] = a['NCUSIP'].str.slice(0, 6)




data_all = os.path.join('E:\\', 'data backup', 'ORS')
list_1 = ['1978', '1981', '1983', 'A-B-C', 'ISTO']
list_a1 = ['07111986-04101991']
list_a2 = ['a04101991-COMAetc']
list_a3 = ['1994-1997']
list_11 = [os.path.join(data_all, i) for i in list_1]
list_aa1 = [os.path.join(data_all, i) for i in list_a1]
list_aa2 = [os.path.join(data_all, i) for i in list_a2]
list_aa3 = [os.path.join(data_all, i) for i in list_a3]

#Old stye
req_paths = []
for i in list_11:
    for path, dirs, files in os.walk(i):
        req_paths.extend([os.path.join(path, i) for i in files])

req_paths2 = []
for i in list_aa1:
    for path, dirs, files in os.walk(i):
        req_paths2.extend([os.path.join(path, i) for i in files])

req_paths3 = [] #if transaction date is empty, replace with middle of reporting month
files_1 = []
for i in list_aa2:
    for path, dirs, files in os.walk(i):
        req_paths3.extend([os.path.join(path, i) for i in files])

req_paths4 = [] #if transaction date is empty, replace with Received Date
for i in list_aa3:
    for path, dirs, files in os.walk(i):
        req_paths4.extend([os.path.join(path, i) for i in files])




n = 0
for i in req_paths:
    if n == 0:
        acomp = pd.read_csv(i, sep=',', encoding="ISO-8859-1")
    if n > 0:
        acomp = pd.concat([acomp, pd.read_csv(i, sep=',', encoding="ISO-8859-1")])
    n += 1


n = 0
for i in req_paths2:
    if n == 0:
        ancomp = pd.read_csv(i, sep=',', encoding="ISO-8859-1")
        ancomp = ancomp.rename(columns=lambda x: x.strip())
        ancomp.columns = ancomp.columns.str.lower()
    if n > 0:
        temp = pd.read_csv(i, sep=',', encoding="ISO-8859-1")
        temp = temp.rename(columns=lambda x: x.strip())
        temp.columns = temp.columns.str.lower()
        ancomp = pd.concat([ancomp, temp])
    n += 1

n = 0
for i in req_paths3:
    if n == 0:
        aancomp = pd.read_csv(i, sep=',', encoding="ISO-8859-1")
    if n > 0:
        aancompaancomp = pd.concat([aancomp, pd.read_csv(i, sep=',', encoding="ISO-8859-1")])
    n += 1

req_paths3[0].split('\\')[-1].split('-')[1][2:4]
req_paths3[0].split('\\')[-1].split('-')[1][0].zfill(2)
def make_date(list_input):
    year = list_input[1][2:4]
    month = list_input[0].zfill(2)
    day = '15'
    newdate = year + month + day
    return newdate
def check_date(inputw):
    year = inputw[0:2]
    month = inputw[2:4]
    day = inputw[4:6]
    if month in ['04','06','09','11'] and int(day) > 30:
        day = '30'
    if month in ['01','03','05','07', '08', '10', '12'] and int(day) > 31:
        day = '31'
    if month in ['02'] and int(day) > 28:
        day = '28'
    return year+month+day
n = 0
for i in req_paths3:
    if n == 0:
        aancomp = pd.read_csv(i, sep=',', encoding="ISO-8859-1")
        #aancomp = aancomp.reindex(sorted(aancomp.columns), axis=1)
        aancomp = aancomp.rename(columns=lambda x: x.strip())
        aancomp.columns = aancomp.columns.str.lower()
        aancomp = aancomp.rename(columns={'insider identity number': 'insider ident.'})
        aancomp['transaction date'].fillna('0', inplace=True)
        aancomp['transaction date'] = aancomp['transaction date'].apply(lambda x: str(x).split('.')[0])
        aancomp['transaction date'] = np.where(aancomp['transaction date'] == '0',
                                               make_date(i.split('\\')[-1].split('-')),
                                               aancomp['transaction date'])
        aancomp['datadate'] = pd.to_datetime(aancomp['transaction date'], format='%y%m%d')
    if n > 0:
        temp = pd.read_csv(i, sep=',', encoding="ISO-8859-1")
        temp = temp.rename(columns=lambda x: x.strip())
        temp.columns = temp.columns.str.lower()
        temp = temp.rename(columns={'insider identity number': 'insider ident.'})
        temp['transaction date'].fillna('0', inplace=True)
        temp['transaction date'] = temp['transaction date'].apply(lambda x: str(x).split('.')[0])
        temp['transaction date'] = np.where(temp['transaction date'] == '0',
                                               make_date(i.split('\\')[-1].split('-')),
                                               temp['transaction date'])
        temp['transaction date'] = np.where(temp['transaction date'].str[4:6] == '00',
                                            temp['transaction date'].str[0:4]+'01',
                                            temp['transaction date'])
        temp['transaction date'] = temp['transaction date'].apply(lambda x: check_date(x))

        temp['datadate'] = pd.to_datetime(temp['transaction date'], format='%y%m%d')
        aancomp = pd.concat([aancomp, temp])
    n += 1

n = 0
for i in req_paths4:
    if n == 0:
        aaancomp = pd.read_csv(i, sep=',', encoding="ISO-8859-1")
        aaancomp = aaancomp.rename(columns=lambda x: x.strip())
        aaancomp.columns = aaancomp.columns.str.lower()
        aaancomp['transaction date'].fillna('0', inplace=True)
        aaancomp['transaction date'] = aaancomp['transaction date'].apply(lambda x: str(x).split('.')[0])
        aaancomp['received date'] = aaancomp['received date'].apply(lambda x: str(x).split('.')[0])
        aaancomp['transaction date'] = np.where(aaancomp['transaction date'] == '0',
                                               aaancomp['received date'],
                                               aaancomp['transaction date'])
        aaancomp['transaction date'] = np.where(aaancomp['transaction date'] == 'None',
                                               aaancomp['received date'],
                                               aaancomp['transaction date'])
        aaancomp['datadate'] = pd.to_datetime(aaancomp['transaction date'], format='%y%m%d')
    if n > 0:
        temp = pd.read_csv(i, sep=',', encoding="ISO-8859-1")
        temp = temp.rename(columns=lambda x: x.strip())
        temp.columns = temp.columns.str.lower()
        temp['transaction date'].fillna('0', inplace=True)
        temp['transaction date'] = temp['transaction date'].apply(lambda x: str(x).split('.')[0])
        temp['received date'] = temp['received date'].apply(lambda x: str(x).split('.')[0])
        temp['transaction date'] = np.where(temp['transaction date'] == '0',
                                               temp['received date'],
                                               temp['transaction date'])
        temp['transaction date'] = np.where(temp['transaction date'] == 'None',
                                               temp['received date'],
                                               temp['transaction date'])
        temp['transaction date'] = np.where(temp['transaction date'].str[4:6] == '00',
                                            temp['transaction date'].str[0:4]+'01',
                                            temp['transaction date'])
        temp['transaction date'] = temp['transaction date'].apply(lambda x: check_date(x))
        temp['datadate'] = pd.to_datetime(temp['transaction date'], format='%y%m%d')
        aaancomp = pd.concat([aaancomp, temp])
    n += 1

acomp = acomp.rename(columns={"Number Used to Identify Issuer of Security": "CUSIP"})
acomp['CUSIP'] = acomp['CUSIP'].astype(str)
acomp['CUSIP'] = acomp['CUSIP'].apply(lambda x: str(x).zfill(6))
acomp = pd.merge(acomp, COMPUCRSPIQCR,
                 left_on=['CUSIP'], right_on=['CNEW6D'], how='left')
acomp['Security Name'].unique()
#turn date into date

check = ['A', 'CLA', 'B', 'CLB', 'COMA', 'COMB', 'C', 'CLC']
nc = ['PFD', 'BD', 'DEB', 'PREF', 'PED', 'WTS', 'WT',
      'BONDS', 'FD', 'VTG', 'INC']


acomp['SN'] = acomp['Security Name'].apply(lambda x: str(x).split(' '))
acomps = acomp[acomp['SN'].apply(lambda x: any(item in x for item in check))]
acomps = acomps[~acomps['SN'].apply(lambda x: any(item in x for item in nc))]
acomps = acomps.dropna(subset=['Date of Transaction'])
acomps = acomps[~acomps['Date of Transaction'].apply(lambda x: x == 'Invalid Date')]
acomps['datadate'] = pd.to_datetime(acomps['Date of Transaction'])

acomps['Security Name'].unique()
a=acomps[acomps['Date of Transaction'].str.contains("Invalid Date")]
acomps[acomps['Date of Transaction'].isna()]

#Match CRSP and acomps by closes
acomps = acomps.dropna(subset=['gvkey'])
acompss = Functions.prep_asof(acomps, 'datadate', 'gvkey', options=0)
acompss['dual_new'] = 1
acompss['gvkey_n'] = acompss['gvkey']
COMPUCRSPIQCRS = Functions.prep_asof(COMPUCRSPIQCRS, 'datadate', 'gvkey', options=0)
COMPUCRSPIQCRS = COMPUCRSPIQCRS.sort_values(by=['tempID'])
acompss = acompss.sort_values(by=['tempID'])
COMPUCRSPIQCRSS = pd.merge_asof(COMPUCRSPIQCRS,
                                acompss[['gvkey_n', 'tempID', 'dual_new', 'datadate']],
                                on='tempID', direction='nearest')

COMPUCRSPIQCRSS = COMPUCRSPIQCRSS[COMPUCRSPIQCRSS.gvkey == COMPUCRSPIQCRSS.gvkey_n]
COMPUCRSPIQCRSS['datediff'] = (COMPUCRSPIQCRSS['datadate_x'] - COMPUCRSPIQCRSS['datadate_y']).dt.days
COMPUCRSPIQCRSS = COMPUCRSPIQCRSS[COMPUCRSPIQCRSS['datediff'] >= -365]
COMPUCRSPIQCRSS = COMPUCRSPIQCRSS[COMPUCRSPIQCRSS['datediff'] <= 1100]
meanlev_1 = COMPUCRSPIQCRSS.groupby(['fyear'])[['dual_new']].count()
meanlev
COMPUCRSPIQCRSS.to_csv(os.path.join(datadirectory, "ORS1.csv.gz"),
                  index=False, compression='gzip')

#now with ancomp
ancomp['datadate'] = pd.to_datetime(ancomp['transaction date'])
ancomp = ancomp[['cusip number', 'datadate']]
aancomps = aancomp[['cusip number', 'datadate']]
aaancomps = aaancomp[['cusip number', 'datadate']]
ancomp = pd.concat([ancomp, aancomps, aaancomps])
ancomps = pd.merge(ancomp, COMPUCRSPIQCR,
                   left_on=['cusip number'],
                   right_on=['CNEW6D'], how='left')

ancomps = ancomps.dropna(subset=['gvkey'])
ancompss = Functions.prep_asof(ancomps, 'datadate', 'gvkey', options=0)
ancompss['dual_new'] = 1
ancompss['gvkey_n'] = ancompss['gvkey']

# COMPUCRSPIQCRS = Functions.prep_asof(COMPUCRSPIQCRS, 'datadate', 'gvkey', options=0)
# COMPUCRSPIQCRS = COMPUCRSPIQCRS.sort_values(by=['tempID'])
ancompss = ancompss.sort_values(by=['tempID'])
COMPUCRSPIQCRSSN = pd.merge_asof(COMPUCRSPIQCRS,
                                ancompss[['gvkey_n', 'tempID', 'dual_new', 'datadate']],
                                on='tempID', direction='nearest')
COMPUCRSPIQCRSSN = COMPUCRSPIQCRSSN[COMPUCRSPIQCRSSN.gvkey == COMPUCRSPIQCRSSN.gvkey_n]
COMPUCRSPIQCRSSN['datediff'] = (COMPUCRSPIQCRSSN['datadate_x'] - COMPUCRSPIQCRSSN['datadate_y']).dt.days
COMPUCRSPIQCRSSN = COMPUCRSPIQCRSSN[COMPUCRSPIQCRSSN['datediff'] >= -365]
COMPUCRSPIQCRSSN = COMPUCRSPIQCRSSN[COMPUCRSPIQCRSSN['datediff'] <= 1100]
meanlev6 = COMPUCRSPIQCRSSN.groupby(['fyear'])[['dual_new']].count()
meanlev
meanlev2
meanlev3
meanlev5

meanlev6
meanlev_1
COMPUCRSPIQCRSSN.to_csv(os.path.join(datadirectory, "ORS2.csv.gz"),
                  index=False)
COMPUCRSPIQCRSS.to_csv(os.path.join(datadirectory, "ORS1.csv.gz"),
                  index=False, compression='gzip')

# Do the other 2
COMPUCRSPIQCRSSN[COMPUCRSPIQCRSSN['gvkey']=='1034']
aaancomp[aaancomp['issuer name'].str.contains("ALPHARMA")]
aaancomps[aaancomps['cusip number'] == '001629']
a=B[B['COMNAM'].str.contains("WANG")]