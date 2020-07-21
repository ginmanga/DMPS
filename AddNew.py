### Add Compustat Balance Sheet Variables not kept in FUNABS
import pandas as pd
import numpy as np
import datetime
import os
import importlib
import Functions

data_all = os.path.join('C:\\', 'Users', 'Panqiao', 'Documents', 'Research', 'Data')
comp = os.path.join(data_all, 'Compustat')
inventories = pd.read_csv(os.path.join(comp, 'inventories', "Compustat-INVENTORIESAR.gz"), sep='\t')
FUNDAP = pd.read_csv(os.path.join(comp, "APWCAP5018.gz"), sep='\t')
FUNDABS = pd.read_csv(os.path.join(comp, "FUNDAMAINBS5018.gz"), sep='\t')
# FUNDADEBT = pd.read_csv(os.path.join(comp, "FUNDADEBT19502018.gz"), sep='\t')


FUNDAP['datadate'] = pd.to_datetime(FUNDAP['datadate'], format='%Y%m%d')
inventories['datadate'] = pd.to_datetime(inventories['datadate'], format='%Y%m%d')
FUNDABS['datadate'] = pd.to_datetime(FUNDABS['datadate'], format='%Y%m%d')
# FUNDADEBT['datadate'] = pd.to_datetime(FUNDADEBT['datadate'], format='%Y%m%d')

FUNDABS = FUNDABS.loc[:, ~FUNDABS.columns.str.endswith('_fn')]
FUNDABS = FUNDABS.loc[:, ~FUNDABS.columns.str.endswith('_dc')]

# FUNDADEBT = FUNDADEBT.loc[:, ~FUNDADEBT.columns.str.endswith('_fn')]
# FUNDADEBT = FUNDADEBT.loc[:, ~FUNDADEBT.columns.str.endswith('_dc')]
# FUNDADEBT = FUNDADEBT[FUNDADEBT.indfmt == 'INDL']

## ADDD TOO

# IQ_SAMPLE.to_csv(os.path.join(datadirectory, "IQ-ready-Jan30.csv.gz"), index=False, compression='gzip')


IQ_SAMPLE = pd.read_csv(os.path.join(datadirectory, "IQ-ready-Mar03.csv.gz"))
BS1DF_COMP = pd.read_csv(os.path.join(datadirectory, "BS1DF-ready-Mar03.csv.gz"))
IQ_SAMPLE['datadate'] = pd.to_datetime(IQ_SAMPLE['datadate'])
BS1DF_COMP['datadate'] = pd.to_datetime(BS1DF_COMP['datadate'])
# Investories variables
# invfg - inventories finished goods
# invo - inventories other
# invrm - inventories raw materials
# invt - inventories - total
# invwip - inventories work in process
# rect receivable total
# rectr receivables trade
# Asset volatility? do later
#wcap from FUNAAP
# CSHO
# prcc_f
# Market capitalization
# from fundadebt get DM and dltp
inventories['invt_a']

ratios = ['invfg', 'invo', 'invrm', 'invt', 'invwip', 'rect', 'rectr']
names = ['INV_FG', 'INV_O', 'INV_RAW', 'INV_TOT', 'INV_WP', 'AREC_TOT', 'AREC_TRA']

# Merge inventories and wcap into FUNDABS
a = ['gvkey', 'datadate']
inventories = pd.merge(inventories, FUNDABS[['gvkey', 'datadate', 'at']], left_on=a, right_on=a)
FUNDAP = pd.merge(FUNDAP, FUNDABS[['gvkey', 'datadate', 'at']], left_on=a, right_on=a)

ratios = ['invfg', 'invo', 'invrm', 'invt', 'invwip', 'rect', 'rectr']
names = ['INV_FG', 'INV_O', 'INV_RAW', 'INV_TOT', 'INV_WP', 'AREC_TOT', 'AREC_TRA']
inventories = Functions.fin_ratio(inventories, ratios, 'at', names=names)
ratios = ['wcap']
names = ['WCAP']
FUNDAP = Functions.fin_ratio(FUNDAP, ratios, 'at', names=names)




#abnormal earnings?
#Mertge Back to IQ_SAMPLE and BS1DF_COMP
inv = ['gvkey', 'datadate', 'INV_FG', 'INV_O', 'INV_RAW', 'INV_TOT', 'INV_WP', 'AREC_TOT', 'AREC_TRA']
BS1DF_COMPc = pd.merge(BS1DF_COMP, inventories[inv], left_on=a, right_on=a, how='left')
IQ_SAMPLEc = pd.merge(IQ_SAMPLE, inventories[inv], left_on=a, right_on=a, how='left')

ap = ['gvkey', 'datadate', 'WCAP']
BS1DF_COMPc = pd.merge(BS1DF_COMPc, FUNDAP[ap], left_on=a, right_on=a, how='left')
IQ_SAMPLEc = pd.merge(IQ_SAMPLEc, FUNDAP[ap], left_on=a, right_on=a, how='left')

list_variables_WINSOR = ['INV_TOT', 'AREC_TOT', 'WCAP']

BS1DF_COMPc = Functions.winsor(BS1DF_COMPc, column=list_variables_WINSOR,
                              cond_list=['NOTMISSING', 'SAMPLE', 'EXCHANGE', 'USCOMMON'],
                              cond_num=[1, 1, 1, 1], quantiles=[0.99, 0.01], year=1968)
IQ_SAMPLEc = Functions.winsor(IQ_SAMPLEc, column=list_variables_WINSOR,
                              cond_list=['NOTMISSING', 'SAMPLE', 'EXCHANGE', 'USCOMMON'],
                              cond_num=[1, 1, 1, 1], quantiles=[0.99, 0.01], year=1968)

NEWBS = ['INV_TOT_cut', 'AREC_TOT_cut', 'WCAP_cut']

BS1DF_COMPc.to_csv(os.path.join(datadirectory, "BS1DF-ready-March04.csv.gz"), index=False, compression='gzip')
IQ_SAMPLEc.to_csv(os.path.join(datadirectory, "IQ-ready-March04.csv.gz"), index=False, compression='gzip')
BS1DF_COMP = BS1DF_COMPc
IQ_SAMPLE = IQ_SAMPLEc
# From debt get maturity like the papers....

# Add governance and ownership
BS1DF_COMP = pd.read_csv(os.path.join(datadirectory, "BS1DF-ready-March04.csv.gz"))
IQ_SAMPLE = pd.read_csv(os.path.join(datadirectory, "IQ-ready-March04.csv.gz"))

gover_legacyID2 = pd.read_csv(os.path.join(data_iss, "gover_legID.gz"))
take_over_index = pd.read_csv(os.path.join(data_takeover, 'takeover_index_lags.csv.gz'))
compensation_g = pd.read_csv(os.path.join(data_takeover, 'compensation.csv.gz'))

# Merge Ownership, gindex and take_over_index
a = ['gvkey', 'fyear']
tindex = ['gvkey', 'fyear', 'hostile_index', 'hostile_index_L1',
          'hostile_index_L2', 'hostile_index_L3', 'hostile_index_L4']
BS1DF_COMP = pd.merge(BS1DF_COMP, take_over_index[tindex], left_on=a, right_on=a, how='left')
IQ_SAMPLE = pd.merge(IQ_SAMPLE, take_over_index[tindex], left_on=a, right_on=a, how='left')

take_vars = ['INVT_TOT', 'AREC_TOT',  'hostile_index', 'hostile_index_L1', 'hostile_index_L2',
            'hostile_index_L3', 'hostile_index_L4' ]

#Governance
a = ['gvkey', 'fyear']
b = ['gvkey', 'YEAR']
gover = ['gvkey', 'fyear', 'DE_INC', 'GINDEX', 'DUALCLASS', 'EINDEX', 'GIGRP_1', 'GIGRP_2']
BS1DF_COMP = pd.merge(BS1DF_COMP, gover_legacyID2[gover], left_on=a, right_on=a, how='left')
IQ_SAMPLE = pd.merge(IQ_SAMPLE, gover_legacyID2[gover], left_on=a, right_on=a, how='left')

gover_vars = ['DE_INC', 'GINDEX', 'DUALCLASS', 'EINDEX', 'GIGRP_1', 'GIGRP_2']

#COMPENSATIOn

a = ['gvkey', 'fyear']
b = ['GVKEY', 'YEAR']
comp = ['GVKEY', 'YEAR', 'PCT_EXCL_OPT_cut', 'PCT_INCL_EX_OPT_cut', 'PCT_INCL_UEX_OPT_cut',
         'OWN_A_1', 'OWN_A_2', 'OWN_B_1', 'OWN_B_2', 'OWN_C_1', 'OWN_C_2']
BS1DF_COMP = pd.merge(BS1DF_COMP, compensation_g[comp], left_on=a, right_on=b, how='left')
IQ_SAMPLE = pd.merge(IQ_SAMPLE, compensation_g[comp], left_on=a, right_on=b, how='left')

comp_vars = ['PCT_EXCL_OPT_cut', 'PCT_INCL_EX_OPT_cut', 'PCT_INCL_UEX_OPT_cut',
             'OWN_A_1', 'OWN_A_2', 'OWN_B_1', 'OWN_B_2', 'OWN_C_1', 'OWN_C_2']


# he olvidado winsorize new ratios


BS1DF_COMP.to_csv(os.path.join(datadirectory, "BS1DF-ready-March04ALL.csv.gz"), index=False, compression='gzip')
IQ_SAMPLE.to_csv(os.path.join(datadirectory, "IQ-ready-March04ALL.csv.gz"), index=False, compression='gzip')

BS1DF_COMP = pd.read_csv(os.path.join(datadirectory, "BS1DF-ready-March04ALL.csv.gz"))
IQ_SAMPLE = pd.read_csv(os.path.join(datadirectory, "IQ-ready-March04ALL.csv.gz"))
# Add debt maturity and market capitalization

# Add instituional ownership

BS1DF_COMP = pd.read_csv(os.path.join(datadirectory, "BS1DF-ready-March04ALL.csv.gz"))
IQ_SAMPLE = pd.read_csv(os.path.join(datadirectory, "IQ-ready-March04ALL.csv.gz"))
COMPUCRSPIQCR = pd.read_csv(os.path.join(datadirectory, "FUNDALIST_March29.csv.gz"))

a = COMPUCRSPIQCR[COMPUCRSPIQCR.gvkey == 2536]
a['COMNAM']


BS1DF_COMP_inst = BS1DF_COMP[BS1DF_COMP['InstOwn_Perc'].notna()]
IQ_SAMPLE_inst = IQ_SAMPLE[IQ_SAMPLE['InstOwn_Perc'].notna()]
BS1DF_COMP_inst = BS1DF_COMP_inst[['gvkey', 'fyear', 'InstOwn_Perc']]
IQ_SAMPLE_inst = IQ_SAMPLE_inst[['gvkey', 'fyear', 'InstOwn_Perc']]

BS1DF_COMP_dir = BS1DF_COMP[BS1DF_COMP['ind_dir_per_ISS'].notna()]
IQ_SAMPLE_dir = IQ_SAMPLE[IQ_SAMPLE['ind_dir_per_ISS'].notna()]
BS1DF_COMP_dir = BS1DF_COMP_dir[['gvkey', 'fyear', 'ind_dir_per_ISS']]
IQ_SAMPLE_dir= IQ_SAMPLE_dir[['gvkey', 'fyear', 'ind_dir_per_ISS']]

BS1DF_COMP_inst = Functions.create_groups(BS1DF_COMP_inst, 'fyear', "InstOwn_Perc", 'INSTOWN', grps=2)
IQ_SAMPLE_inst = Functions.create_groups(IQ_SAMPLE_inst, 'fyear', "InstOwn_Perc", 'INSTOWN', grps=2)
BS1DF_COMP_dir = Functions.create_groups(BS1DF_COMP_dir, 'fyear', "ind_dir_per_ISS", 'board-independence', grps=2)
IQ_SAMPLE_dir = Functions.create_groups(IQ_SAMPLE_dir, 'fyear', "ind_dir_per_ISS", 'board-independence', grps=2)



a = ['gvkey', 'fyear']
comp = ['gvkey', 'fyear', 'INSTOWN_1', 'INSTOWN_2']

BS1DF_COMP = pd.merge(BS1DF_COMP, BS1DF_COMP_inst[comp], left_on=a, right_on=a, how='left')
IQ_SAMPLE = pd.merge(IQ_SAMPLE, IQ_SAMPLE_inst[comp], left_on=a, right_on=a, how='left')

comp = ['gvkey', 'fyear', 'board-independence_1', 'board-independence_2']
BS1DF_COMP = pd.merge(BS1DF_COMP, BS1DF_COMP_dir[comp], left_on=a, right_on=a, how='left')
IQ_SAMPLE = pd.merge(IQ_SAMPLE, IQ_SAMPLE_dir[comp], left_on=a, right_on=a, how='left')

BS1DF_COMP.to_csv(os.path.join(datadirectory, "BS1DF-ready-March30ALL.csv.gz"), index=False, compression='gzip')
IQ_SAMPLE.to_csv(os.path.join(datadirectory, "IQ-ready-March30ALL.csv.gz"), index=False, compression='gzip')

#Add dualdata and capital liquidity
BS1DF_COMP = pd.read_csv(os.path.join(datadirectory, "BS1DF-ready-March30ALL.csv.gz"))
IQ_SAMPLE = pd.read_csv(os.path.join(datadirectory, "IQ-ready-March30ALL.csv.gz"))
capital_liquidity = capital_liquidity[['date', 'cl_4q_avg']]
dualdata = dualdata[['GVKEY', 'YEAR']]
dualdata['dual_stock'] = 1

dualdata = dualdata.dropna(subset=['GVKEY'])
dualdata = dualdata[dualdata.GVKEY != '.']
dualdata['GVKEY'] = dualdata['GVKEY'].astype(int)


BS1DF_COMP_short = BS1DF_COMP[(BS1DF_COMP['fyear'] >= 1994) & (BS1DF_COMP['fyear'] <= 2002)]
BS1DF_COMP_short = BS1DF_COMP_short[['gvkey', 'fyear']]
BS1DF_COMP_short = pd.merge(BS1DF_COMP_short, dualdata, left_on=['gvkey', 'fyear'],
                      right_on=['GVKEY', 'YEAR'], how='left')
BS1DF_COMP_short['dual_stock'] = np.where(BS1DF_COMP_short['dual_stock'].isna(), 0, 1)
BS1DF_COMP_short = BS1DF_COMP_short[['gvkey','fyear', 'dual_stock']]
BS1DF_COMP = pd.merge(BS1DF_COMP, BS1DF_COMP_short, left_on=['gvkey', 'fyear'],
                      right_on=['gvkey', 'fyear'], how='left')

capital_liquidity.dtypes
BS1DF_COMP['datadate'].dtypes
BS1DF_COMP['datadate'] = pd.to_datetime(BS1DF_COMP['datadate'])
IQ_SAMPLE['datadate'] = pd.to_datetime(IQ_SAMPLE['datadate'])
capital_liquidity['datadate'] = pd.to_datetime(capital_liquidity['date'])
IQ_SAMPLE_short = IQ_SAMPLE[['gvkey','datadate']]
IQ_SAMPLE_short = IQ_SAMPLE_short.sort_values(by=['datadate'])
BS1DF_COMP_short = BS1DF_COMP[['gvkey','datadate']]
BS1DF_COMP_short = BS1DF_COMP_short.sort_values(by=['datadate'])
BS1DF_COMP_short = pd.merge_asof(BS1DF_COMP_short, capital_liquidity,
                        left_on=['datadate'], right_on=['datadate'],
                        direction='nearest')
IQ_SAMPLE_short = pd.merge_asof(IQ_SAMPLE_short, capital_liquidity,
                        left_on=['datadate'], right_on=['datadate'],
                        direction='nearest')
BS1DF_COMP_short = BS1DF_COMP_short[['gvkey', 'datadate', 'cl_4q_avg']]
IQ_SAMPLE_short = IQ_SAMPLE_short[['gvkey', 'datadate', 'cl_4q_avg']]
BS1DF_COMP = pd.merge(BS1DF_COMP, BS1DF_COMP_short, left_on=['gvkey', 'datadate'],
                      right_on=['gvkey', 'datadate'], how='left')
IQ_SAMPLE = pd.merge(IQ_SAMPLE, IQ_SAMPLE_short, left_on=['gvkey', 'datadate'],
                      right_on=['gvkey', 'datadate'], how='left')


BS1DF_COMP.to_csv(os.path.join(datadirectory, "BS1DF-ready-APRIL16ALL.csv.gz"), index=False, compression='gzip')
IQ_SAMPLE.to_csv(os.path.join(datadirectory, "IQ-ready-APRIL16ALL.csv.gz"), index=False, compression='gzip')

# Add dual dta from ORS
dualdata
dualdata.groupby(['YEAR'])[['GVKEY']].count()
BS1DF_COMP = pd.read_csv(os.path.join(datadirectory, "BS1DF-ready-APRIL16ALL.csv.gz"))
#first, joing ORS1 and ORS2 into a single file
ORS2 = pd.read_csv(os.path.join(datadirectory, "ORS2.csv.gz"))
ORS1 = pd.read_csv(os.path.join(datadirectory, "ORS1.csv.gz"))

ORS1 = ORS1[['gvkey', 'fyear', 'PERMNO', 'dual_new']]
ORS2 = ORS2[['gvkey', 'fyear', 'PERMNO', 'dual_new']]
ORS = pd.merge(ORS1, ORS2, on=['gvkey', 'fyear'], how='outer')
ORS = ORS.sort_values(by=['gvkey', 'fyear'])
dual_short = dualdata[['GVKEY', 'YEAR']]
dual_short = dual_short.rename(columns={"GVKEY": "gvkey", 'YEAR': 'fyear'})
dual_short['dual_new_z'] = 1

dual_short = dual_short.dropna(subset=['gvkey'])
dual_short = dual_short[dual_short.gvkey != '.']
dual_short['gvkey'] = dual_short['gvkey'].astype(int)

ORS = pd.merge(ORS, dual_short, on=['gvkey', 'fyear'], how='outer')
ORS = ORS.sort_values(by=['gvkey', 'fyear'])
ORS['dual'] = ORS['dual_new_x']
ORS['dual'] = np.where(ORS['dual_new_x'].isna(),
                       ORS['dual_new_y'], ORS['dual'])
ORS['dual'] = np.where(ORS['dual'].isna(),
                       ORS['dual_new_z'], ORS['dual'])

BS1DF_COMP_short = BS1DF_COMP[(BS1DF_COMP['fyear'] >= 1978) & (BS1DF_COMP['fyear'] <= 2002)]
BS1DF_COMP_short = BS1DF_COMP_short[['gvkey', 'fyear']]
BS1DF_COMP_short = pd.merge(BS1DF_COMP_short, ORS[['gvkey', 'fyear', 'dual']],
                            left_on=['gvkey', 'fyear'], right_on=['gvkey', 'fyear'], how='left')
BS1DF_COMP_short['dual'] = np.where(BS1DF_COMP_short['dual'].isna(), 0, 1)
BS1DF_COMP_short = BS1DF_COMP_short[['gvkey','fyear', 'dual']]
BS1DF_COMP = pd.merge(BS1DF_COMP, BS1DF_COMP_short, left_on=['gvkey', 'fyear'],
                      right_on=['gvkey', 'fyear'], how='left')
BS1DF_COMP.to_csv(os.path.join(datadirectory, "BS1DF-ready-APRIL20ALL.csv.gz"),
                  index=False, compression='gzip')
BS1DF_COMP = pd.read_csv(os.path.join(datadirectory, "BS1DF-ready-APRIL20ALL.csv.gz"))

a=BS1DF_COMP.groupby(['fyear'])[['dual']].mean()
ORS1[ORS1.PERMNO==13777]

ORS1.groupby(['fyear'])[['dual_new']].count()

#Add from dual ipo data
#dualipo data do as follows, if data already in BS1DF
dualipo['year'] = dualipo['Offer date'].apply(lambda x: str(x)[0:4])
COMPUCRSPIQCR = COMPUCRSPIQCR.drop_duplicates(subset=['PERMNO'])
COMPUCRSPIQCR = COMPUCRSPIQCR[['gvkey', 'PERMNO']]
BS1DF_COMPs = BS1DF_COMP[['gvkey', 'datadate', 'fyear', 'dual']]
dualipo['CRSP perm'] = np.where(dualipo['CRSP perm'] == '.', np.nan, dualipo['CRSP perm'])
dualipo = dualipo.dropna(subset=['CRSP perm'])
dualipo = dualipo.astype({'CRSP perm': 'int64'})
dualipo = pd.merge(dualipo, COMPUCRSPIQCR, left_on=['CRSP perm'],
                      right_on=['PERMNO'], how='left')

dualipo.groupby(['year'])[['dual dum']].count()
dualipo
BS1DF_COMPs01 = BS1DF_COMPs[BS1DF_COMPs.fyear == 2001]
BS1DF_COMPs01 = BS1DF_COMPs01[BS1DF_COMPs01.dual == 1]
BS1DF_COMPs01 = BS1DF_COMPs01[['gvkey', 'dual']]
BS1DF_COMPs01 = BS1DF_COMPs01.rename(columns={"dual": "dualn"})

BS1DF_COMPs = pd.merge(BS1DF_COMPs, dualipo[['gvkey', 'Offer date', 'dual dum']],
                       left_on=['gvkey'], right_on=['gvkey'], how='left')
BS1DF_COMPs = pd.merge(BS1DF_COMPs, BS1DF_COMPs01,
                       left_on=['gvkey'], right_on=['gvkey'], how='left')


BS1DF_COMPss = BS1DF_COMPs[BS1DF_COMPs['dual dum'] == 1]
BS1DF_COMPsss = BS1DF_COMPs[BS1DF_COMPs['dual'] == 1]

BS1DF_COMPs['dual'].fillna(0, inplace=True)
BS1DF_COMPs['dual dum'].fillna(0, inplace=True)
BS1DF_COMPs['dualn'].fillna(0, inplace=True)

BS1DF_COMPs['dual_ext'] = BS1DF_COMPs['dual'] + BS1DF_COMPs['dual dum'] + BS1DF_COMPs['dualn']
BS1DF_COMPs['dual_ext'] = np.where(BS1DF_COMPs['dual_ext'] > 1, 1, BS1DF_COMPs['dual_ext'])


BS1DF_COMPs.groupby(['fyear'])[['dual_ext']].mean()

BS1DF_COMP = pd.merge(BS1DF_COMP, BS1DF_COMPs[['gvkey', 'fyear', 'dual_ext']],
                      left_on=['gvkey', 'fyear'], right_on=['gvkey', 'fyear'], how='left')
BS1DF_COMP.to_csv(os.path.join(datadirectory, "BS1DF-ready-APRIL24ALL.csv.gz"),
                  index=False, compression='gzip')

BS1DF_COMP = pd.read_csv(os.path.join(datadirectory, "BS1DF-ready-APRIL24ALL.csv.gz"))
a = BS1DF_COMP[BS1DF_COMP.gvkey == 2536]
a['COMNAM']