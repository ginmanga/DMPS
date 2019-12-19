# FUNDABS main database
import numpy as np
import pandas as pd
import Functions
import os

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
#  CAPX capital expendiotrues
# XSGA selling general and admin expenses
# XRD research and development
# CEQ common/ordinary equity  total
# APALCH accounts payable and AL increase decrewase
# RECCH accounts receivable decrease
# WCAP working capital
# dltis long-term debt issuance
# dltr long-term debt reduction
# DLCCH current debt changes
#  SSTK sale of common and preferred stock
# PRSTKC purchase of common and preferred stock

# merge with fundadebt for total debt and with COMPUCRSPIQCR for LINKDT to calculate age
#from debt get TOTALDEBT SUB SBN BD CL SHORT CHECK HH1 HH2


datadirectory = os.path.join(os.getcwd(), 'data')

FUNDADEBT = pd.read_csv(os.path.join(datadirectory, "fundadebtprocessedDEC18.csv.gz"))
FUNDADEBT['datadate'] = pd.to_datetime(FUNDADEBT['datadate'])

c = ['gvkey', 'datadate', 'TOTALDEBT_C']
FUNDABS = pd.merge(FUNDABS,
                   FUNDADEBT[c],
                   left_on=['gvkey', 'datadate'],
                   right_on=['gvkey', 'datadate'],
                   how='left')

# get sic_ch and FF48
FUNDASIC = pd.read_csv(os.path.join(datadirectory, 'processed', "FUNDASIC_DEC4.gz"), index_col=0)
FUNDASIC['datadate'] = pd.to_datetime(FUNDASIC['datadate'])

FUNDABS = pd.merge(FUNDABS,
                   FUNDASIC[['gvkey', 'datadate', 'sic_ch', 'FF48']],
                   left_on=['gvkey', 'datadate'],
                   right_on=['gvkey', 'datadate'],
                   how='left')

# Erase fin and utilities
f2 = list(range(4900, 4949))  # delete utilities
f3 = list(range(6000, 6999))  # delete utilities
FUNDABS['util'] = [1 if x in f2 else 0 for x in FUNDABS['sic_ch']]
FUNDABS['fin'] = [1 if x in f3 else 0 for x in FUNDABS['sic_ch']]
# FUNDABS = FUNDABS[FUNDABS.util == 0]
# FUNDABS = FUNDABS[FUNDABS.fin == 0]
# FUNDABS = FUNDABS.drop(columns=['util','fin'])
# Calculate STD of sales and earnings
FUNDABS['PROF'] = FUNDABS['oibdp']/FUNDABS['at']
FUNDABS['SALE'] = FUNDABS['sale']/FUNDABS['at']
list_variables_WINSOR = ['PROF', 'SALE']
FUNDABS = Functions.winsor(FUNDABS, column=list_variables_WINSOR)

FUNDABS, c = Functions.rol_vars(FUNDABS, 'SALE_cut', 'sale_std_ff48_4', group=['gvkey', 'datadate'], onn='datadate',
                                window=4, levels=2, group1=['gvkey', 'datadate', 'fyear', 'FF48'],
                                group2=['FF48', 'fyear'])

# c_bs = ['gvkey','datadate', 'fyear', 'ap', 'at','sale','prcc_f', 'cshfd', 'pstkl', 'txditc', 'oibdp',
# 'dvc','dvt','che','ppent','capx','xsga','xrd','ceq', 'TOTALDEBT_C']

c_bs= ['gvkey','datadate', 'fyear', 'ap', 'at', 'sale', 'prcc_f', 'cshfd', 'pstkl', 'txditc', 'oibdp',
       'dvc', 'dvt', 'che', 'ppent', 'capx', 'xsga', 'xrd', 'ceq', 'TOTALDEBT_C', 'util', 'fin', 'sale_std_ff48_4']

#Replace missing with 0 or remove
list_replace = ['ap', 'cshfd', 'pstkl', 'txditc', 'oibdp',
                'dvc', 'dvt', 'che', 'ppent', 'capx', 'xsga', 'xrd', 'ceq']

list_remove = ['at', 'TOTALDEBT_C', 'prcc_f']

BS1DF = FUNDABS[c_bs]

len(BS1DF) #  512059

for i in list_replace:
    BS1DF[i].fillna(0, inplace=True)

for i in list_remove:
    BS1DF = BS1DF.dropna(subset=[i])

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

# build balance sheet variables
BS1DF['MVEquity'] = BS1DF['prcc_f']*BS1DF['cshfd']
BS1DF['MVBook'] = (BS1DF['MVEquity']+BS1DF['TOTALDEBT_C'] -
                   BS1DF['pstkl'] - BS1DF['txditc'])/BS1DF['at']
BS1DF['DIVP'] = np.where(BS1DF['dvc'] > 0, 1, 0)  # dividend payer
BS1DF['MLEV'] = BS1DF['TOTALDEBT_C']/(BS1DF['TOTALDEBT_C'] + BS1DF['MVEquity'])


ratios = ['oibdp', 'che', 'ppent', 'capx', 'xsga', 'xrd', 'ap', 'TOTALDEBT_C']
names = ['PROF', 'CASH', 'TANG', 'CAPEX', 'ADVERT', 'RD', 'AP', 'BLEV']
BS1DF = Functions.fin_ratio(BS1DF, ratios, 'at', names=names)

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

FUNDAQ = pd.read_csv(os.path.join(datadirectory, 'processed', "FUNDAQDEC8STD.gz"))
FUNDAQ['datadate'] = pd.to_datetime(FUNDAQ['datadate'])

to_keep = ['gvkey', 'datadate', 'sale_std_12', 'sale_std_9', 'sale_std_ff48_12', 'sale_std_ff48_9',
           'income_std_12', 'income_std_9']

FUNDABS = pd.merge(FUNDABS,
                   FUNDAQ[to_keep],
                   left_on=['gvkey', 'datadate'],
                   right_on=['gvkey', 'datadate'], how='left')



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
len(BS1DF)  # 351182　351182
BS1DF.to_csv(os.path.join(datadirectory, "BS1DF-temp.csv.gz"), index=False, compression='gzip')

BS1DF = pd.read_csv(os.path.join(datadirectory, "BS1DF-temp.csv"))
BS1DF['datadate'] = pd.to_datetime(BS1DF['datadate'])

# BS1DF = BS1DF.drop(columns=['sic_ch_x','FF48_x'])
# BS1DF.rename(columns={'sic_ch_y':'sic_ch','FF48_y':'FF48' }, inplace=True)
# Translate quarter into date and match back to BS1DF
c = pd.read_csv(os.path.join(datadirectory, "FF48SALE12.gz"))
c['datadate'] = c['datacqtr'].str.replace(r'(\d+)(Q\d)', r'\1-\2')
# c['datadate'] = pd.PeriodIndex(c['datadate'], freq='Q').to_timestamp()
c['datadate'] = pd.to_datetime(c['datadate'])

# MATCH USING CLOSEST

# BS1DF = BS1DF.drop(columns=['temp_x', 'tempdays_x', 'temp_y','tempdays_y'])
# BS1DF.rename(columns={'datadate_x': 'datadate'}, inplace=True)
# BS1DF.rename(columns={'FF48': 'FF48'}, inplace=True)
# BS1DF_small = BS1DF[BS1DF.fyear < 1980]
cc = c[['FF48', 'datadate', 'sale_std_ff48_12']]  # why the hell? Different one
BS1DF = Functions.match_closest(BS1DF, cc, 'FF48', 'datadate', direction='nearest')
BS1DF = BS1DF.sort_values(by=['gvkey', 'datadate'])
BS1DF.rename(columns={'sale_std_ff48_12_x': 'sale_std_ff48_12_1', 'sale_std_ff48_12_y': 'sale_std_ff48_12_2'},
             inplace=True)

BS1DF.to_csv(os.path.join(datadirectory, "BS1DF-temp2.csv.gz"), index=False, compression='gzip')
BS1DF = pd.read_csv(os.path.join(datadirectory, "BS1DF-temp2.csv.gz"))
BS1DF['datadate'] = pd.to_datetime(BS1DF['datadate'])

# Add crsp EXCH and nation

FUNDALIST_CRSPIDS = pd.read_csv(os.path.join(datadirectory, 'processed', "FUNDALIST_CRSPIDSDEC4.gz"), index_col=0)
FUNDALIST_CRSPIDS = FUNDALIST_CRSPIDS.sort_values(by=['gvkey', 'datadate'])
FUNDALIST_CRSPIDS['datadate'] = pd.to_datetime(FUNDALIST_CRSPIDS['datadate'])

# CRSP:
# EXCHCD 1 NYSE, 2 AMS, 3 NASDAQ, 4 ARCA, -2 halted by NYSE AMX -1 suspended  0 Not trading NYSE AMEX NASDAQ
# SHRCD 10 or 11 (12 incorp outside the US)

BS1DF = pd.merge(BS1DF, FUNDALIST_CRSPIDS[['gvkey', 'datadate', 'SHRCD', 'EXCHCD', 'splticrm']],
                 left_on=['gvkey', 'datadate'],
                 right_on=['gvkey', 'datadate'], how='left')


# Finish preparing compustat sample,

BS1DF = Functions.rating_grps(BS1DF)
BS1DF.to_csv(os.path.join(datadirectory, "BS1DF.csv.gz"), index=False, compression='gzip')

BS1DF = pd.read_csv(os.path.join(datadirectory, "BS1DF.csv.gz"))
BS1DF['datadate'] = pd.to_datetime(BS1DF['datadate'])
# Merge with FUNDADEBT, keep only u.S AMX NYSE NASDAQ

BS1DF['EXCHANGE'] = np.where((BS1DF.EXCHCD == 1) | (BS1DF.EXCHCD == 2) | (BS1DF.EXCHCD == 3), 1, 0)
BS1DF['USCOMMON'] = np.where((BS1DF.SHRCD == 10) | (BS1DF.SHRCD == 11), 1, 0)

FUNDADEBT = pd.read_csv(os.path.join(datadirectory, "fundadebtprocessedDEC18.csv.gz"))
FUNDADEBT['datadate'] = pd.to_datetime(FUNDADEBT['datadate'])


var_debt_comp = ['gvkey', 'datadate', 'SUB_C', 'SBN_C', 'BD_C', 'CL_C', 'SHORT_C',
                 'OTHER_C', 'BDB_C', 'SUBNOTCONV_C', 'SUBCONV_C', 'CONV_C', 'DD_C',
                 'DN_C', 'SHORT_CTEMP', 'OTHERA_C', 'SUBCONV_CTEMP', 'HH1', 'HH2', 'HH1B', 'HH2B',
                 'SUB_CPCT', 'SBN_CPCT', 'BD_CPCT', 'CL_CPCT', 'SHORT_CPCT', 'cmpPCT', 'OTHERA_CPCT',
                 'SUBNOTCONV_CPCT', 'SUBCONV_CPCT', 'CONV_CPCT', 'DD_CPCT', 'DN_CPCT', 'TOTALDEBT_C_2',
                 'NP_Exact', 'NP_OVER', 'NP_UNDER', 'NPOU_Exact', 'DEBTSUM_ERR', 'KEEP1', 'KEEP2', 'KEEP3']

var_debt_comp = ['gvkey', 'datadate', 'SUB_C', 'SBN_C', 'BD_C', 'CL_C', 'SHORT_C', 'OTHER_C', 'BDB_C',
                 'SUBNOTCONV_C', 'SUBCONV_C', 'CONV_C', 'DD_C', 'DN_C', 'HH1', 'HH2', 'SUB_CPCT', 'SBN_CPCT',
                 'BD_CPCT', 'CL_CPCT', 'SHORT_CPCT', 'SUBNOTCONV_CPCT', 'SUBCONV_CPCT', 'CONV_CPCT', 'DD_CPCT',
                 'DN_CPCT', 'TOTALDEBT_C_2', 'NP_Exact', 'NP_OVER', 'NP_UNDER', 'NPOU_Exact', 'DEBTSUM_ERR', 'KEEP_E']


#######################
BS1DF_COMP = pd.merge(BS1DF, FUNDADEBT[var_debt_comp], left_on=['gvkey', 'datadate'],
                      right_on=['gvkey', 'datadate'], how='left')
# BS1DF_COMP = Functions.rating_grps(BS1DF)


len(BS1DF_COMP)  # 353611 353611
BS1DF_COMP = BS1DF_COMP[BS1DF_COMP.util == 0]
BS1DF_COMP = BS1DF_COMP[BS1DF_COMP.fin == 0]
BS1DF_COMP = BS1DF_COMP.drop(columns=['util', 'fin'])
len(BS1DF_COMP)  # 282510


# Change to dummy variable called sample
# len(BS1DF_COMP) # 28510 267984
# BS1DF_COMP = BS1DF_COMP[BS1DF_COMP.TOTALDEBT_C_2 > 0]
BS1DF_COMP['sample'] = np.where((BS1DF_COMP.TOTALDEBT_C_2 > 0) & (BS1DF_COMP.HH1 <= 1)
                                & (BS1DF_COMP.fyear >= 1969), 1, 0)
len(BS1DF_COMP)  # 228601  217794 243132 #239010
# B S1DF_COMP = BS1DF_COMP[BS1DF_COMP.HH1 >= 0]
BS1DF_COMP['HH1'] = np.where(BS1DF_COMP.HH1 < 0, np.NaN, BS1DF_COMP.HH1)
BS1DF_COMP['HH2'] = np.where(BS1DF_COMP.HH2 < 0, np.NaN, BS1DF_COMP.HH2)
BS1DF_COMP['HH1'] = np.where(BS1DF_COMP.HH1 > 1, np.NaN, BS1DF_COMP.HH1)
BS1DF_COMP['HH2'] = np.where(BS1DF_COMP.HH2 > 1, np.NaN, BS1DF_COMP.HH2)
# BS1DF_COMP= BS1DF_COMP[BS1DF_COMP.HH1 <= 1]
len(BS1DF_COMP)  # 228583 214190

BS1DF_COMP = BS1DF_COMP[BS1DF_COMP.fyear < 2019]
len(BS1DF_COMP)  # 228583 214190

# BS1DF_COMP_S = BS1DF_COMP[(BS1DF_COMP['sample'] == 1) & (BS1DF_COMP['EXCHANGE'] == 1) & (BS1DF_COMP['USCOMMON'] == 1)]
# BS1DF_COMP_S = BS1DF_COMP[BS1DF_COMP.fyear < 1986]
# BS1DF_COMP_S['PROF'].quantile(0.99)
# BS1DF_COMP_S['PROF'].quantile(0.01)
# BS1DF_COMP['PROF'].quantile(0.99)
# BS1DF_COMP_S['MVBook'].quantile(0.99)

# BS1DF_COMP['PROF'].quantile(0.99)
# BS1DF_COMP['PROF'].quantile(0.05)

list_variables_WINSOR = ['MVBook', 'PROF', 'CASH', 'TANG', 'CAPEX', 'ADVERT', 'RD', 'MLEV', 'BLEV', 'AP']

BS1DF_COMP = Functions.winsor(BS1DF_COMP, column=list_variables_WINSOR,
                              cond_list=['sample', 'EXCHANGE', 'USCOMMON'],
                              cond_num=[1, 1, 1], quantiles=[0.99, 0.01], year=1968)

# BS1DF_COMP['PROF_cut'].describe()
# BS1DF_COMP['MVBook_cut'].describe()

# list_variables_WINSOR = ['MVBook', 'PROF', 'CASH', 'TANG', 'CAPEX', 'ADVERT', 'RD', 'MLEV', 'BLEV', 'AP']
BS1DF_COMP = BS1DF_COMP.drop(list_variables_WINSOR, axis=1)
list_variables_re = [i + '_cut' for i in list_variables_WINSOR]
for i in list_variables_re:
    BS1DF_COMP.rename(columns={i: i.replace('_cut', '')}, inplace=True)

# BS1DF_COMP = BS1DF_COMP[(BS1DF_COMP.EXCHCD == 1)| (BS1DF_COMP.EXCHCD == 2) | (BS1DF_COMP.EXCHCD == 3)]
# len(BS1DF_COMP) #174740 165431

# BS1DF_COMP = BS1DF_COMP[(BS1DF_COMP.SHRCD == 10)| (BS1DF_COMP.SHRCD == 11)]
# len(BS1DF_COMP) #156014 144308 158165

BS1DF_COMP.to_csv(os.path.join(datadirectory, "BS1DF-ready-Dec19.csv.gz"), index=False, compression='gzip')


BS1DF_COMP = BS1DF_COMP[BS1DF_COMP.D != 1]
len(BS1DF_COMP)  # 124355 121157


##################################
##################################
##################################
# CAPITAL IQ ################
##################################
##################################

var_debt_capiq = ['gvkey', 'datadate', 'HH1', 'TOTALDEBT_C_2', 'NP_OVER', 'NP_UNDER',
                  'NPOU_Exact', 'DEBTSUM_ERR', 'KEEP_E']

var_debt_capiq = ['gvkey', 'datadate', 'SUB_C', 'SBN_C', 'BD_C', 'CL_C', 'SHORT_C', 'OTHER_C',
                  'BDB_C', 'SUBNOTCONV_C', 'SUBCONV_C', 'CONV_C', 'DD_C', 'DN_C', 'HH1', 'HH2',
                  'SUB_CPCT', 'SBN_CPCT', 'BD_CPCT', 'CL_CPCT', 'SHORT_CPCT', 'SUBNOTCONV_CPCT',
                  'SUBCONV_CPCT', 'CONV_CPCT', 'DD_CPCT', 'DN_CPCT', 'TOTALDEBT_C_2', 'NP_Exact',
                  'NP_OVER', 'NP_UNDER', 'NPOU_Exact', 'DEBTSUM_ERR', 'KEEP_E']

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

list_types_iq = ['CP_IQ', 'DC_IQ', 'TL_IQ', 'SBN_IQ', 'SUB_IQ', 'CL_IQ', 'OTHER_IQ']
for i in list_types_iq:
    IQ_SAMPLE[i].fillna(0, inplace=True)

# Calculate HH1

ts = ['OutstandingBalrRevolvingCredit', 'SrBondsandNotes', 'SubordinatedBondsandNotes', 'OutstandingBalCapitalLeases',
      'TotOutstBalCommercialPaper', 'GeneralOtherBorrowings', 'OutstandingBalTermLoans', 'TotTrustPreferred',
      'TotAdjustments']

IQ_SAMPLE['sumdebt'] = IQ_SAMPLE[ts].sum(axis=1)
IQ_SAMPLE['CHECK_IQ'] = (IQ_SAMPLE['sumdebt']-IQ_SAMPLE['TOTALDEBT_C'])/(IQ_SAMPLE['TOTALDEBT_C'])

len(IQ_SAMPLE)  # 52157

list_types_iq = ['CP_IQ', 'DC_IQ', 'TL_IQ', 'SBN_IQ', 'SUB_IQ', 'CL_IQ', 'OTHER_IQ']

IQ_SAMPLE = Functions.hhi_calculator(list_types_iq, 'sumdebt', 'HH1_IQ', IQ_SAMPLE)
IQ_SAMPLE = Functions.hhi_calculator(list_types_iq, 'TOTALDEBT_C', 'HH2_IQ', IQ_SAMPLE)

len(IQ_SAMPLE)  # 52157
IQ_SAMPLE = IQ_SAMPLE[IQ_SAMPLE.CHECK_IQ <= 0.1]
len(IQ_SAMPLE)  # 44934 44929
IQ_SAMPLE = IQ_SAMPLE[IQ_SAMPLE.CHECK_IQ >= -0.1]
len(IQ_SAMPLE)  # 43738 43738 43733

# Change to dummy variable called sample

IQ_SAMPLE['sample'] = np.where((IQ_SAMPLE.sumdebt > 0) & (IQ_SAMPLE.HH1_IQ >= 0) &
                               (BS1DF_COMP.fyear >= 2002) & (BS1DF_COMP.at > 0) & (BS1DF_COMP.prcc_f > 0), 1, 0)


IQ_SAMPLE['HH1_IQ'] = np.where(IQ_SAMPLE.HH1_IQ < 0, np.NaN, IQ_SAMPLE.HH1_IQ)
IQ_SAMPLE['HH2_IQ'] = np.where(IQ_SAMPLE.HH2_IQ < 0, np.NaN, IQ_SAMPLE.HH2_IQ)
IQ_SAMPLE['HH1_IQ'] = np.where(IQ_SAMPLE.HH1_IQ > 1, np.NaN, IQ_SAMPLE.HH1_IQ)
IQ_SAMPLE['HH2_IQ'] = np.where(IQ_SAMPLE.HH2_IQ > 1, np.NaN, IQ_SAMPLE.HH2_IQ)

len(BS1DF_COMP)  # 228583 214190

IQ_SAMPLE = IQ_SAMPLE.replace([np.inf, -np.inf], np.nan)

IQ_SAMPLE = IQ_SAMPLE[IQ_SAMPLE.fyear < 2019]
len(IQ_SAMPLE)  # 228583 214190

IQ_SAMPLE = IQ_SAMPLE[IQ_SAMPLE.util == 0]
IQ_SAMPLE = IQ_SAMPLE[IQ_SAMPLE.fin == 0]
IQ_SAMPLE = IQ_SAMPLE.drop(columns=['util', 'fin'])

list_variables_WINSOR = ['MVBook', 'PROF', 'CASH', 'TANG', 'CAPEX', 'ADVERT', 'RD', 'MLEV', 'BLEV', 'AP']
IQ_SAMPLE = Functions.winsor(IQ_SAMPLE, column=list_variables_WINSOR,
                             cond_list=['sample', 'EXCHANGE', 'USCOMMON'],
                             cond_num=[1, 1, 1], quantiles=[0.99, 0.01], year=2001)

IQ_SAMPLE = IQ_SAMPLE.drop(list_variables_WINSOR, axis=1)
list_variables_re = [i + '_cut' for i in list_variables_WINSOR]
for i in list_variables_re:
    IQ_SAMPLE.rename(columns={i: i.replace('_cut', '')}, inplace=True)

IQ_SAMPLE.to_csv(os.path.join(datadirectory, "IQ-ready-DEC19.csv.gz"), index=False, compression='gzip')


##############################################
##############################################






IQ_SAMPLE = IQ_SAMPLE[IQ_SAMPLE.fyear < 2019]
len(IQ_SAMPLE)  # 38816 33787


BS1DF_COMP = BS1DF_COMP[BS1DF_COMP.HH1_C >= 0]
len(BS1DF_COMP)  # 229257
BS1DF_COMP= BS1DF_COMP[BS1DF_COMP.HH1_C <= 1]
len(BS1DF_COMP)  # 228023

BS1DF_COMP = BS1DF_COMP[BS1DF_COMP.fyear < 2019]
len(BS1DF_COMP)  # 228023





len(BS1DF_COMP)  # 150490

BS1DF_COMP.to_csv(os.path.join(datadirectory, "BS1DF-ready.csv.gz"), index=False, compression='gzip')


BS1DF_COMP = BS1DF_COMP[BS1DF_COMP.D != 1]
len(BS1DF_COMP)  # 124355 121157

# add inflation to calculate deflated assets