# FUNDABS main database
import numpy as np
import pandas as pd
import os
import importlib
import Functions
importlib.reload(Functions)


FUNDABS = 0
#  AT Total assets
# Sale
# PRCC_F
# CSHFD common shares used to cal earning per share
# csho common shares outstanding
# PSTKL preferred stock liquidating value
# TXDITC deferred taxes and investment tax credit
# OIBDP operating income before depreciation
# DVC dividends common
# DVT dividends total
# CHE cash and short-term investments
# PPENT proerty etc
# CAPX capital expendiotrues
# XSGA selling general and admin expenses
# XRD research and development
# CEQ common/ordinary equity  total
#AP accounts payable
# APALCH accounts payable and AL increase decrewase
#rect recievables total
# tectr recevables trade
# RECCH accounts receivable decrease
# WCAP working capital
#invt inventories total
#invfg finished goods
#invrm inventories raw
# dltis long-term debt issuance
# dltr long-term debt reduction
# DLCCH current debt changes
# SSTK sale of common and preferred stock
# PRSTKC purchase of common and preferred stock
#pstkc - preferred stock conv
# pstk preferred stockj total
# pstkrv preferred stock redemption value

# from debt get TOTALDEBT SUB SBN BD CL SHORT CHECK HH1 HH2


datadirectory = os.path.join(os.getcwd(), 'data')

FUNDADEBT = pd.read_csv(os.path.join(datadirectory, "fundadebtprocessedJAN30.csv.gz"))
FUNDADEBT['datadate'] = pd.to_datetime(FUNDADEBT['datadate'])

c = ['gvkey', 'datadate', 'TOTALDEBT_C']
FUNDABS = pd.merge(FUNDABS, FUNDADEBT[c], left_on=['gvkey', 'datadate'], right_on=['gvkey', 'datadate'], how='left')

# get sic_ch and FF48
FUNDASIC = pd.read_csv(os.path.join(datadirectory, 'processed', "FUNDASIC_DEC4.gz"), index_col=0)
FUNDASIC['datadate'] = pd.to_datetime(FUNDASIC['datadate'])

FUNDABS = pd.merge(FUNDABS, FUNDASIC[['gvkey', 'datadate', 'sic_ch', 'FF48']], left_on=['gvkey', 'datadate'],
                   right_on=['gvkey', 'datadate'], how='left')

#Make dummies for SIC single digit



# Erase fin and utilities
f2 = list(range(4900, 4949))  # delete utilities
f3 = list(range(6000, 6999))  # delete utilities
FUNDABS['util'] = [1 if x in f2 else 0 for x in FUNDABS['sic_ch']]
FUNDABS['fin'] = [1 if x in f3 else 0 for x in FUNDABS['sic_ch']]

# Calculate STD of sales and earnings
FUNDABS['PROF'] = FUNDABS['oibdp']/FUNDABS['at']
FUNDABS['SALE'] = FUNDABS['sale']/FUNDABS['at']
list_variables_WINSOR = ['PROF', 'SALE']
FUNDABS = Functions.winsor(FUNDABS, column=list_variables_WINSOR, cond_list=['util', 'fin'], cond_num=[0, 0],
                           quantiles=[0.999, 0.001], year=1960)

FUNDABS, c = Functions.rol_vars(FUNDABS, 'SALE', 'sale_std_4', group=['gvkey', 'datadate'], onn='datadate',
                                window=4, levels=1)

FUNDABS, c = Functions.rol_vars(FUNDABS, 'SALE_cut', 'cut_sale_std_4', group=['gvkey', 'datadate'], onn='datadate',
                                window=4, levels=1)

FUNDABS, c = Functions.rol_vars(FUNDABS, 'SALE_cut', 'sale_std_ff48_4', group=['gvkey', 'datadate'], onn='datadate',
                                window=4, levels=2, group1=['gvkey', 'datadate', 'fyear', 'FF48'],
                                group2=['FF48', 'fyear'])


FUNDABS, c = Functions.rol_vars(FUNDABS, 'PROF', 'income_std_4', group=['gvkey', 'datadate'], onn='datadate',
                                window=4, levels=1)

FUNDABS, c = Functions.rol_vars(FUNDABS, 'PROF_cut', 'cut_income_std_4', group=['gvkey', 'datadate'], onn='datadate',
                                window=4, levels=1)


#FUNDABS, c = Functions.rol_vars(FUNDABS, 'PROF', 'income_std_10_FF48', group=['gvkey', 'datadate'], onn='datadate',
                                #window=10, levels=2, group1=['gvkey', 'datadate', 'fyear', 'FF48'],
                                #group2=['FF48', 'fyear'])

# FUNDABS, c = Functions.rol_vars(FUNDABS, 'PROF_cut', 'cut_income_std_10_FF48', group=['gvkey', 'datadate'],
                                # onn='datadate', window=10, levels=2, group1=['gvkey', 'datadate', 'fyear', 'FF48'],
                                # group2=['FF48', 'fyear'])

# c_bs = ['gvkey', 'datadate', 'fyear','fic', 'ap', 'at', 'sale', 'prcc_f', 'cshfd', 'cshpri', 'pstkl', 'txditc', 'oibdp',
        # 'dvc', 'dvt', 'che', 'ppent', 'capx', 'xsga', 'xrd', 'ceq', 'TOTALDEBT_C',  'util', 'fin', 'sale_std_ff48_4',
        # 'income_std_4', 'cut_income_std_4', 'sale_std_4', 'cut_sale_std_4', 'income_std_10_FF48',
        # 'cut_income_std_10_FF48']

c_bs = ['gvkey', 'datadate', 'fyear','fic', 'ap', 'at', 'sale', 'prcc_f', 'cshfd', 'cshpri', 'pstkl', 'txditc', 'oibdp',
        'dvc', 'dvt', 'che', 'ppent', 'capx', 'xsga', 'xrd', 'ceq', 'TOTALDEBT_C',  'util', 'fin', 'sale_std_ff48_4',
        'income_std_4', 'cut_income_std_4', 'sale_std_4', 'cut_sale_std_4']
# Replace missing with 0 or remove
list_replace = ['ap', 'pstkl', 'txditc', 'dvc', 'dvt', 'che', 'ppent', 'capx', 'xsga', 'xrd', 'ceq']


list_replace = ['pstkl', 'txditc', 'dvc', 'dvt', 'ppent', 'capx', 'xsga', 'xrd', 'ceq']


BS1DF = FUNDABS[c_bs]

print(len(BS1DF))  # 512059

for i in list_replace:
    BS1DF[i].fillna(0, inplace=True)

len(BS1DF)  # 351182
##########################################
# Add Inflation and deflate AT, t.....####
#########################################
inflation = pd.read_csv(os.path.join(datadirectory, "CPIAUCSL.csv"))
inflation['DATE'] = pd.to_datetime(inflation['DATE'])
inflation.rename(columns={'DATE': 'datadate'}, inplace=True)
BS1DF = BS1DF.sort_values(by=['datadate'])
BS1DF = pd.merge_asof(BS1DF, inflation[['datadate', 'NEWDOLLARS']], on=['datadate'], direction='nearest')
BS1DF = BS1DF.sort_values(by=['gvkey', 'datadate'])

BS1DF['AT'] = BS1DF['at']*BS1DF['NEWDOLLARS']
BS1DF = BS1DF.drop(columns=['NEWDOLLARS'])
BS1DF['SIZE'] = np.log(BS1DF['AT'])

# build balance sheet variables
BS1DF['MVEquity'] = BS1DF['prcc_f']*BS1DF['cshpri']
BS1DF['MVBook'] = (BS1DF['MVEquity']+BS1DF['TOTALDEBT_C'] -
                   BS1DF['pstkl'] - BS1DF['txditc'])/BS1DF['at']
BS1DF['DIVP'] = np.where(BS1DF['dvc'] > 0, 1, 0)  # dividend payer
BS1DF['MLEV'] = BS1DF['TOTALDEBT_C']/(BS1DF['TOTALDEBT_C'] + BS1DF['MVEquity'])
#Add Working capital net of cash

ratios = ['oibdp', 'che', 'ppent', 'capx', 'xsga', 'xrd', 'ap', 'TOTALDEBT_C']
names = ['PROF', 'CASH', 'TANG', 'CAPEX', 'ADVERT', 'RD', 'AP', 'BLEV']
BS1DF = Functions.fin_ratio(BS1DF, ratios, 'at', names=names)
BS1DF = BS1DF.replace([np.inf, -np.inf], np.nan)
list_variables = ['MVEquity', 'MVBook', 'PROF', 'DIVP', 'CASH', 'TANG', 'CAPEX', 'ADVERT', 'RD', 'MLEV', 'BLEV']


# for later: product uniqueness, CF volatility, asset maturity

# list_variables_WINSOR = ['MVBook', 'PROF','CASH', 'TANG', 'CAPEX', 'ADVERT', 'RD', 'MLEV', 'BLEV', 'AP']
# BS1DF = Functions.winsor(BS1DF, column = list_variables_WINSOR)

# Add and remove variables only the needed ones
# delete original ratios and rename the cuts
# list_variables_WINSOR = ['MVBook', 'PROF','CASH', 'TANG', 'CAPEX', 'ADVERT', 'RD', 'MLEV', 'BLEV', 'AP']
# BS1DF = BS1DF.drop(list_variables_WINSOR, axis=1)
# list_variables_re = [i + '_cut' for i in list_variables_WINSOR]
# for i in list_variables_re:
#    BS1DF.rename(columns={i: i.replace('_cut','')}, inplace=True

# Add volatility variables
# First add quartelry ones to FUNDABS, then add them to BS1DF
# FUNDAQ.to_csv(os.path.join(datadirectory, "FUNDAQDEC8STD.csv.gz"), index=False, compression='gzip')

FUNDAQ = pd.read_csv(os.path.join(datadirectory, "FUNDAQDEC22STD.csv.gz"))
FUNDAQ['datadate'] = pd.to_datetime(FUNDAQ['datadate'])

to_keep = ['gvkey', 'datadate', 'sale_std_12', 'sale_std_9', 'sale_std_ff48_12', 'sale_std_ff48_9',
           'income_std_12', 'income_std_9']

FUNDABS = pd.merge(FUNDABS, FUNDAQ[to_keep], left_on=['gvkey', 'datadate'], right_on=['gvkey', 'datadate'], how='left')

# Calculate age using all compustat
a = FUNDABS.groupby(['gvkey']).cumcount().reset_index(name='counts')
a['counts'] = a['counts'] + 1
ce = FUNDABS.join(a)
ce.rename(columns={'counts': 'AGE'}, inplace=True)

to_keep = ['gvkey', 'datadate', 'sale_std_12', 'sale_std_9', 'sale_std_ff48_12', 'sale_std_ff48_9',
           'income_std_12', 'income_std_9', 'FF48', 'sic_ch']

BS1DF = pd.merge(BS1DF, FUNDABS[to_keep], left_on=['gvkey', 'datadate'], right_on=['gvkey', 'datadate'], how='left')


BS1DF = pd.merge(BS1DF, ce[['gvkey', 'datadate', 'AGE']], left_on=['gvkey', 'datadate'],
                 right_on=['gvkey', 'datadate'], how='left')
len(BS1DF)  #512059 351182　351182
BS1DF.to_csv(os.path.join(datadirectory, "BS1DF-temp.csv.gz"), index=False, compression='gzip')

BS1DF = pd.read_csv(os.path.join(datadirectory, "BS1DF-temp.csv"))
BS1DF['datadate'] = pd.to_datetime(BS1DF['datadate'])

# Translate quarter into date and match back to BS1DF
c = pd.read_csv(os.path.join(datadirectory, "FF48SALE12DEC22.gz"))
c['datadate'] = c['datacqtr'].str.replace(r'(\d+)(Q\d)', r'\1-\2')
# c['datadate'] = pd.PeriodIndex(c['datadate'], freq='Q').to_timestamp()
c['datadate'] = pd.to_datetime(c['datadate'])

# MATCH USING CLOSEST


cc = c[['FF48', 'datadate', 'sale_std_ff48_12']]  # why the hell? Different one
BS1DF = BS1DF.dropna(subset=['FF48'])
BS1DF = Functions.match_closest(BS1DF, cc, 'FF48', 'datadate', direction='nearest')
BS1DF = BS1DF.sort_values(by=['gvkey', 'datadate'])
BS1DF.rename(columns={'sale_std_ff48_12_x': 'sale_std_ff48_12_1', 'sale_std_ff48_12_y': 'sale_std_ff48_12_2'},
             inplace=True)


BS1DF.to_csv(os.path.join(datadirectory, "BS1DF-temp2.csv.gz"), index=False, compression='gzip')

BS1DF = pd.read_csv(os.path.join(datadirectory, "BS1DF-temp2.csv.gz"))
BS1DF['datadate'] = pd.to_datetime(BS1DF['datadate'])

# Add crsp EXCH and nation

## CHANGE THIS ONE TO THE NEW

FUNDALIST_CRSPIDS_old = pd.read_csv(os.path.join(datadirectory, 'processed', "FUNDALIST_CRSPIDSDEC4.gz"), index_col=0)
FUNDALIST_CRSPIDS = pd.read_csv(os.path.join(datadirectory, 'processed', "FUNDALIST_CRSPIDSFEB28.csv.gz"), index_col=0)
FUNDALIST_CRSPIDS = FUNDALIST_CRSPIDS.sort_values(by=['gvkey', 'datadate'])
FUNDALIST_CRSPIDS['datadate'] = pd.to_datetime(FUNDALIST_CRSPIDS['datadate'])

# CRSP:
# EXCHCD 1 NYSE, 2 AMS, 3 NASDAQ, 4 ARCA, -2 halted by NYSE AMX -1 suspended  0 Not trading NYSE AMEX NASDAQ
# SHRCD 10 or 11 (12 incorp outside the US)

BS1DF = pd.merge(BS1DF, FUNDALIST_CRSPIDS[['gvkey', 'datadate', 'SHRCD', 'EXCHCD', 'splticrm']],
                 left_on=['gvkey', 'datadate'],
                 right_on=['gvkey', 'datadate'], how='left')


# Finish preparing compustat sample,
# BS1DF.splticrm.unique()
BS1DF['splticrm'] = np.where((BS1DF.fyear >= 1986) & (BS1DF['splticrm'].isnull()), '0', BS1DF['splticrm'])
BS1DF = Functions.rating_grps(BS1DF)

BS1DF['EXCHANGE'] = np.where((BS1DF.EXCHCD == 1) | (BS1DF.EXCHCD == 2) | (BS1DF.EXCHCD == 3), 1, 0)
BS1DF['USCOMMON'] = np.where((BS1DF.SHRCD == 10) | (BS1DF.SHRCD == 11), 1, 0)
BS1DF['NOTMISSING'] = np.where(BS1DF['at'].notna() & BS1DF['prcc_f'].notna() & BS1DF['TOTALDEBT_C'].notna(), 1, 0)

BS1DF.to_csv(os.path.join(datadirectory, "BS1DF-March03.csv.gz"), index=False, compression='gzip')

BS1DF = pd.read_csv(os.path.join(datadirectory, "BS1DF.csv.gz"))
BS1DF = pd.read_csv(os.path.join(datadirectory, "BS1DF-March03.csv.gz"))
BS1DF['datadate'] = pd.to_datetime(BS1DF['datadate'])
# Merge with FUNDADEBT, keep only u.S AMX NYSE NASDAQ

# list_remove = ['at', 'TOTALDEBT_C', 'prcc_f']
# for i in list_remove:  BS1DF = BS1DF.dropna(subset=[i])

FUNDADEBT = pd.read_csv(os.path.join(datadirectory, "fundadebtprocessedJan30.csv.gz"))
FUNDADEBT['datadate'] = pd.to_datetime(FUNDADEBT['datadate'])
# SS = FUNDADEBT[FUNDADEBT['gvkey'] == 10984]

var_debt_comp = ['gvkey', 'datadate', 'SUB_C', 'SBN_C', 'BD_C', 'CL_C', 'SHORT_C',
                 'OTHER_C', 'BDB_C', 'SUBNOTCONV_C', 'SUBCONV_C', 'CONV_C', 'DD_C',
                 'DN_C', 'SHORT_CTEMP', 'OTHERA_C', 'SUBCONV_CTEMP', 'HH1', 'HH2', 'HH1B', 'HH2B',
                 'SUB_CPCT', 'SBN_CPCT', 'BD_CPCT', 'CL_CPCT', 'SHORT_CPCT', 'cmpPCT', 'OTHERA_CPCT',
                 'SUBNOTCONV_CPCT', 'SUBCONV_CPCT', 'CONV_CPCT', 'DD_CPCT', 'DN_CPCT', 'TOTALDEBT_C_2',
                 'NP_Exact', 'NP_OVER', 'NP_UNDER', 'NPOU_Exact', 'DEBTSUM_ERR', 'KEEP1', 'KEEP2', 'KEEP3']

var_debt_comp = ['gvkey', 'datadate', 'SUB_C', 'SBN_C', 'BD_C', 'CL_C', 'SHORT_C', 'OTHER_C', 'BDB_C',
                 'SUBNOTCONV_C', 'SUBCONV_C', 'CONV_C', 'DD_C', 'DN_C', 'cmp', 'dd1', 'dlc', 'HH1', 'HH2', 'SUB_CPCT',
                 'SBN_CPCT', 'BD_CPCT', 'CL_CPCT', 'SHORT_CPCT', 'SUBNOTCONV_CPCT', 'SUBCONV_CPCT', 'CONV_CPCT',
                 'DD_CPCT', 'DN_CPCT', 'TOTALDEBT_C_2', 'NP_Exact', 'NP_OVER', 'NP_UNDER', 'NPOU_Exact',
                 'DEBTSUM_ERR', 'KEEP_E', 'dm', 'dltp']


#######################
BS1DF_COMP = pd.merge(BS1DF, FUNDADEBT[var_debt_comp], left_on=['gvkey', 'datadate'],
                      right_on=['gvkey', 'datadate'], how='left')
# BS1DF_COMP = Functions.rating_grps(BS1DF)


len(BS1DF_COMP)  # 514492 353611 353611
BS1DF_COMP = BS1DF_COMP[BS1DF_COMP.util == 0]
BS1DF_COMP = BS1DF_COMP[BS1DF_COMP.fin == 0]
BS1DF_COMP = BS1DF_COMP.drop(columns=['util', 'fin'])
len(BS1DF_COMP)  # 378866 378866 282510

# len(BS1DF_COMP) # 28510 267984

BS1DF_COMP['SAMPLE'] = np.where((BS1DF_COMP.TOTALDEBT_C_2 > 0) & (BS1DF_COMP.HH1 <= 1.1)
                                & (BS1DF_COMP.fyear >= 1969) & (BS1DF_COMP.TOTALDEBT_C >= 0), 1, 0)

BS1DF_COMP['SAMPLE'] = np.where((BS1DF_COMP.TOTALDEBT_C_2 > 0) & (BS1DF_COMP.HH1 <= 1.1)
                                & (BS1DF_COMP.fyear >= 1969) & (BS1DF_COMP.TOTALDEBT_C >= 0), 1, 0)

len(BS1DF_COMP)  # 228601  217794 243132 #239010
S = BS1DF_COMP[BS1DF_COMP.HH1 > 1.1]
BS1DF_COMP['HH1'] = np.where(BS1DF_COMP.HH1 < 0, np.NaN, BS1DF_COMP.HH1)
BS1DF_COMP['HH2'] = np.where(BS1DF_COMP.HH2 < 0, np.NaN, BS1DF_COMP.HH2)
BS1DF_COMP['HH1'] = np.where(BS1DF_COMP.HH1 > 1.1, np.NaN, BS1DF_COMP.HH1)
BS1DF_COMP['HH2'] = np.where(BS1DF_COMP.HH2 > 1.1, np.NaN, BS1DF_COMP.HH2)
BS1DF_COMP['HH1'] = np.where(BS1DF_COMP.HH1 > 1, 1, BS1DF_COMP.HH1)
BS1DF_COMP['HH2'] = np.where(BS1DF_COMP.HH2 > 1, 1, BS1DF_COMP.HH2)

print(len(BS1DF_COMP))  # 378866 228583 214190
BS1DF_COMP = BS1DF_COMP[BS1DF_COMP.fyear < 2019]
print(len(BS1DF_COMP))  # 378396 378866 228583 214190

BS1DF_COMP = BS1DF_COMP.replace([np.inf, -np.inf], np.nan)

# BS1DF_COMP['income_std_12_at'] = BS1DF_COMP['income_std_12']/BS1DF_COMP['at'] need fix
# BS1DF_COMP['income_std_9_at'] = BS1DF_COMP['income_std_9']/BS1DF_COMP['at'] need fix

list_variables_WINSOR = ['MVBook', 'PROF', 'CASH', 'TANG', 'CAPEX', 'ADVERT', 'RD', 'MLEV', 'BLEV', 'AP', 'AT',
                         'income_std_12', 'income_std_9', 'sale_std_12', 'sale_std_9', 'income_std_4',
                         'cut_income_std_4']
BS1DF_COMP = BS1DF_COMP.drop_duplicates()
print(len(BS1DF_COMP))  # 376347 376348 376817

BS1DF_COMP = Functions.winsor(BS1DF_COMP, column=list_variables_WINSOR,
                              cond_list=['NOTMISSING', 'SAMPLE', 'EXCHANGE', 'USCOMMON'],
                              cond_num=[1, 1, 1, 1], quantiles=[0.99, 0.01], year=1968)



BS1DF_COMP.to_csv(os.path.join(datadirectory, "BS1DF-ready-Mar03.csv.gz"), index=False, compression='gzip')

BS1DF_COMP.to_csv(os.path.join(datadirectory, "BS1DF-ready-Jan30.csv.gz"), index=False, compression='gzip')
BS1DF_COMP = BS1DF_COMP[BS1DF_COMP.D != 1]
len(BS1DF_COMP)  # 124355 121157


##################################
##################################
##################################
# CAPITAL IQ ################
##################################
##################################

FUNDADEBT = pd.read_csv(os.path.join(datadirectory, "fundadebtprocessedJan30.csv.gz"))
FUNDADEBT['datadate'] = pd.to_datetime(FUNDADEBT['datadate'])

var_debt_capiq = ['gvkey', 'datadate', 'SUB_C', 'SBN_C', 'BD_C', 'CL_C', 'SHORT_C', 'OTHER_C', 'BDB_C', 'SUBNOTCONV_C',
                  'SUBCONV_C', 'CONV_C', 'DD_C', 'DN_C', 'cmp', 'dd1', 'dlc', 'HH1', 'HH2', 'SUB_CPCT', 'SBN_CPCT',
                  'BD_CPCT', 'CL_CPCT', 'SHORT_CPCT', 'SUBNOTCONV_CPCT', 'SUBCONV_CPCT', 'CONV_CPCT', 'DD_CPCT',
                  'DN_CPCT', 'TOTALDEBT_C_2', 'NP_Exact', 'NP_OVER', 'NP_UNDER', 'NPOU_Exact', 'DEBTSUM_ERR', 'KEEP_E',
                  'dm', 'dltp']

ALLIDSMERGED = pd.read_csv(os.path.join(datadirectory, 'processed', "CAPIIQGVKEYM.csv"), index_col=0)
ALLIDSMERGED['datadate'] = pd.to_datetime(ALLIDSMERGED['datadate'])


IQ_SAMPLE = pd.merge(ALLIDSMERGED, BS1DF, left_on=['gvkey', 'datadate'], right_on=['gvkey', 'datadate'], how='left')
IQ_SAMPLE = pd.merge(IQ_SAMPLE, FUNDADEBT[var_debt_capiq], left_on=['gvkey', 'datadate'],
                     right_on=['gvkey', 'datadate'], how='left')

# Rename variables
IQ_SAMPLE['CP_IQ'] = IQ_SAMPLE['TotOutstBalCommercialPaper']
IQ_SAMPLE['DC_IQ'] = IQ_SAMPLE['OutstandingBalrRevolvingCredit']
IQ_SAMPLE['TL_IQ'] = IQ_SAMPLE['OutstandingBalTermLoans']
IQ_SAMPLE['SBN_IQ'] = IQ_SAMPLE['SrBondsandNotes']
IQ_SAMPLE['SUB_IQ'] = IQ_SAMPLE['SubordinatedBondsandNotes']
IQ_SAMPLE['CL_IQ'] = IQ_SAMPLE['OutstandingBalCapitalLeases']
IQ_SAMPLE['OTHER_IQ'] = (IQ_SAMPLE['GeneralOtherBorrowings'] + IQ_SAMPLE['TotTrustPreferred'])
IQ_SAMPLE['BD_IQ'] = IQ_SAMPLE['TL_IQ'] + IQ_SAMPLE['DC_IQ']

list_types_iq = ['CP_IQ', 'DC_IQ', 'TL_IQ', 'SBN_IQ', 'SUB_IQ', 'CL_IQ', 'OTHER_IQ', 'BD_IQ']
for i in list_types_iq:
    IQ_SAMPLE[i].fillna(0, inplace=True)

# Calculate HH1

ts = ['OutstandingBalrRevolvingCredit', 'SrBondsandNotes', 'SubordinatedBondsandNotes', 'OutstandingBalCapitalLeases',
      'TotOutstBalCommercialPaper', 'GeneralOtherBorrowings', 'OutstandingBalTermLoans', 'TotTrustPreferred',
      'TotAdjustments']

ts = ['OutstandingBalrRevolvingCredit', 'SrBondsandNotes', 'SubordinatedBondsandNotes', 'OutstandingBalCapitalLeases',
      'TotOutstBalCommercialPaper', 'GeneralOtherBorrowings', 'OutstandingBalTermLoans', 'TotTrustPreferred']

IQ_SAMPLE['sumdebt'] = IQ_SAMPLE[ts].sum(axis=1)
IQ_SAMPLE['CHECK_IQ'] = (IQ_SAMPLE['sumdebt']-IQ_SAMPLE['TOTALDEBT_C'])/(IQ_SAMPLE['TOTALDEBT_C'])
S = IQ_SAMPLE[(IQ_SAMPLE.CHECK_IQ > 0.1) | (IQ_SAMPLE.CHECK_IQ < -0.1)]
IQ_SAMPLE = IQ_SAMPLE.replace([np.inf, -np.inf], np.nan)
IQ_SAMPLE['CHECK_IQ'] = IQ_SAMPLE['CHECK_IQ'].abs()
S = IQ_SAMPLE[(IQ_SAMPLE.CHECK_IQ > 0.1)]
len(IQ_SAMPLE)  # 52157

list_types_iq = ['CP_IQ', 'DC_IQ', 'TL_IQ', 'SBN_IQ', 'SUB_IQ', 'CL_IQ', 'OTHER_IQ']
list_types_iq2 = ['CP_IQ', 'BD_IQ', 'SBN_IQ', 'SUB_IQ', 'CL_IQ', 'OTHER_IQ']
IQ_SAMPLE = Functions.hhi_calculator(list_types_iq, 'sumdebt', 'HH1_IQ', IQ_SAMPLE)
IQ_SAMPLE = Functions.hhi_calculator(list_types_iq2, 'sumdebt', 'HH2_IQ', IQ_SAMPLE)
IQ_SAMPLE = Functions.hhi_calculator(list_types_iq, 'TOTALDEBT_C', 'HH3_IQ', IQ_SAMPLE)
IQ_SAMPLE = Functions.hhi_calculator(list_types_iq2, 'TOTALDEBT_C', 'HH4_IQ', IQ_SAMPLE)

# Change to dummy variable called sample

# IQ_SAMPLE['SAMPLE'] = np.where((IQ_SAMPLE.sumdebt > 0) & (IQ_SAMPLE.HH1_IQ >= 0) &
# (IQ_SAMPLE.fyear >= 2002) & (IQ_SAMPLE.CHECK_IQ <= 0.1)
# & (IQ_SAMPLE.CHECK_IQ >= -0.1), 1, 0)

IQ_SAMPLE['SAMPLE'] = np.where((IQ_SAMPLE.HH1_IQ <= 1.1) & (IQ_SAMPLE.TOTALDEBT_C >= 0) & (IQ_SAMPLE.HH1_IQ >= 0)
                               & (IQ_SAMPLE.fyear >= 2002) & (IQ_SAMPLE.CHECK_IQ <= 0.1), 1, 0)

len(IQ_SAMPLE)  # 73322
IQ_SAMPLE = IQ_SAMPLE[IQ_SAMPLE.util == 0]
IQ_SAMPLE = IQ_SAMPLE[IQ_SAMPLE.fin == 0]
IQ_SAMPLE = IQ_SAMPLE.drop(columns=['util', 'fin'])
len(IQ_SAMPLE)  # 69732

# len(BS1DF_COMP) # 28510 267984

IQ_SAMPLE['HH1_IQ'] = np.where(IQ_SAMPLE.HH1_IQ < 0, np.NaN, IQ_SAMPLE.HH1_IQ)
IQ_SAMPLE['HH2_IQ'] = np.where(IQ_SAMPLE.HH2_IQ < 0, np.NaN, IQ_SAMPLE.HH2_IQ)
IQ_SAMPLE['HH1_IQ'] = np.where(IQ_SAMPLE.HH1_IQ > 1.1, np.NaN, IQ_SAMPLE.HH1_IQ)
IQ_SAMPLE['HH2_IQ'] = np.where(IQ_SAMPLE.HH2_IQ > 1.1, np.NaN, IQ_SAMPLE.HH2_IQ)
IQ_SAMPLE['HH1_IQ'] = np.where(IQ_SAMPLE.HH1_IQ > 1, 1, IQ_SAMPLE.HH1_IQ)
IQ_SAMPLE['HH2_IQ'] = np.where(IQ_SAMPLE.HH2_IQ > 1, 1, IQ_SAMPLE.HH2_IQ)

IQ_SAMPLE['HH1'] = np.where(IQ_SAMPLE.HH1 < 0, np.NaN, IQ_SAMPLE.HH1)
IQ_SAMPLE['HH2'] = np.where(IQ_SAMPLE.HH2 < 0, np.NaN, IQ_SAMPLE.HH2)
IQ_SAMPLE['HH1'] = np.where(IQ_SAMPLE.HH1 > 1.1, np.NaN, IQ_SAMPLE.HH1)
IQ_SAMPLE['HH2'] = np.where(IQ_SAMPLE.HH2 > 1.1, np.NaN, IQ_SAMPLE.HH2)
IQ_SAMPLE['HH1'] = np.where(IQ_SAMPLE.HH1 > 1, 1, IQ_SAMPLE.HH1)
IQ_SAMPLE['HH2'] = np.where(IQ_SAMPLE.HH2 > 1, 1, IQ_SAMPLE.HH2)

# IQ_SAMPLE['income_std_12_at'] = IQ_SAMPLE['income_std_12']/(IQ_SAMPLE['at'])
# IQ_SAMPLE['income_std_9_at'] = IQ_SAMPLE['income_std_9']/(IQ_SAMPLE['at'])
len(IQ_SAMPLE)  # 228583 214190
IQ_SAMPLE = IQ_SAMPLE.replace([np.inf, -np.inf], np.nan)
IQ_SAMPLE = IQ_SAMPLE[IQ_SAMPLE.fyear < 2019]
len(IQ_SAMPLE)  # 228583 214190


print(IQ_SAMPLE['PROF'].quantile(0.99))
print(IQ_SAMPLE['PROF'].max())
print(IQ_SAMPLE['PROF'].quantile(0.01))
print(IQ_SAMPLE['PROF'].min())

list_variables_WINSOR = ['MVBook', 'PROF', 'CASH', 'TANG', 'CAPEX', 'ADVERT', 'RD', 'MLEV', 'BLEV', 'AP', 'AT',
                         'income_std_12', 'income_std_9', 'sale_std_12', 'sale_std_9', 'income_std_4',
                         'cut_income_std_4']
IQ_SAMPLE = IQ_SAMPLE.drop_duplicates()
len(IQ_SAMPLE)  # 66521
IQ_SAMPLE = Functions.winsor(IQ_SAMPLE, column=list_variables_WINSOR,
                             cond_list=['NOTMISSING', 'SAMPLE', 'EXCHANGE', 'USCOMMON'],
                             cond_num=[1, 1, 1, 1], quantiles=[0.99, 0.01], year=2001)

print(IQ_SAMPLE['PROF_cut'].quantile(0.99))
print(IQ_SAMPLE['PROF_cut'].max())
print(IQ_SAMPLE['PROF_cut'].quantile(0.01))
print(IQ_SAMPLE['PROF_cut'].min())

IQ_SAMPLE.to_csv(os.path.join(datadirectory, "IQ-ready-Mar03.csv.gz"), index=False, compression='gzip')


IQ_SAMPLE.to_csv(os.path.join(datadirectory, "IQ-ready-Jan30.csv.gz"), index=False, compression='gzip')
