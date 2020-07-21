####Compile Execucomp Data nad add to main analysis file
import os
import pandas as pd
import numpy as np
data_all = os.path.join('C:\\', 'Users', 'Panqiao', 'Documents', 'Research', 'Data')
data_execo = os.path.join(data_all, 'execucomp') #gvkey dataset
data_iss = os.path.join(data_all, 'ISS', 'Governance')
data_takeover = os.path.join(data_all, 'Takeover index')
data_owner = os.path.join(data_all, 'WRDS TR Ownership data')
data_dir = os.path.join(data_all, 'ISS', 'Directors')
boardex = os.path.join(data_all, 'BoardEx', 'NA')
acomp = pd.read_csv(os.path.join(data_execo, 'Anual compensation', "annual compensation.gz"), sep='\t',
                    encoding="ISO-8859-1")
gover_legacy = pd.read_csv(os.path.join(data_iss, "Governance Legacy Data Request.gz"), sep='\t',
                           encoding="ISO-8859-1") #ticker CUSIP 6 igit and CIK
gover_new = pd.read_csv(os.path.join(data_iss, "Governance.gz"), sep='\t', encoding="ISO-8859-1")

TRINST = pd.read_csv(os.path.join(data_owner, "8e886292874f9189.csv.gz"), sep=',', encoding="ISO-8859-1")
capital_liquidityc
take_over_index = pd.read_csv(os.path.join(data_takeover, 'takeover_index.csv'), sep=',', encoding="ISO-8859-1") #gvkey
capital_liquidity = pd.read_csv(os.path.join(data_takeover, 'capital_liquidityc.csv'), sep=',', encoding="ISO-8859-1")
dualdata = pd.read_csv(os.path.join(data_takeover, 'duakdata.csv'), sep=',', encoding="ISO-8859-1")
EINDEX = pd.read_csv(os.path.join(data_takeover, 'EINDEX.txt'), sep='\t', encoding="ISO-8859-1")
eindexdc = pd.read_csv(os.path.join(data_takeover, 'EINDEXDUALCLASS.txt'), sep='\t', encoding="ISO-8859-1")
board_comp = pd.read_csv(os.path.join(boardex, 'organization summary', 'na_wrds_org_composition.gz'), sep='\t', encoding="ISO-8859-1")
board_comp_profile = pd.read_csv(os.path.join(boardex, 'companyprofile.txt.gz'), sep='\t', encoding="ISO-8859-1")
dualipo = pd.read_csv(os.path.join(data_takeover, 'dualipo.csv'), sep=',', encoding="ISO-8859-1")
# Directors
dir_legacy = pd.read_csv(os.path.join(data_dir, "ISS - Directors Legacy Data Request.gz"), sep='\t',
                           encoding="ISO-8859-1") #ticker CUSIP 6 igit and CIK
dir_new = pd.read_csv(os.path.join(data_dir, "ISS - Directors Data Request.gz"), sep='\t', encoding="ISO-8859-1")
##### Merged File COMPUSTAT CRSP ########
FUNDALIST_CRSPIDS = pd.read_csv(os.path.join(datadirectory, 'processed', "FUNDALIST_ALLFEB28.csv.gz")) #, index_col=0)
vars = ['gvkey', 'datadate', 'date', 'fyear', 'LPERMNO', 'PERMNO', 'COMNAM', 'CUSIP', 'NCUSIP', 'TICKER']
MAINID = FUNDALIST_CRSPIDS[vars]
MAINID[MAINID.COMNAM == 'AGILENT TECHNOLOGIES INC']
MAINID[MAINID.COMNAM == 'AMERICAN ANNUITY GROUP INC']
MAINID[MAINID.TICKER == 'AAG']
M = MAINID[MAINID.TICCRSP == 'ACK']
MAINID[MAINID.gvkey == 1762]
M = MAINID[MAINID.gvkey == 1240]
MM = MAINID[MAINID.gvkey == 1240]
MAINID[MAINID.COMNAM == 'THE ADVISORY BOARD CO']
MAINID=MAINID.rename(columns={'TICKER': 'TICCRSP'})
########

# Kepp variables we need, match with FUNDALIST_CRSPIDS and get gvkey and permno for ones without it
# Match back with the result from FUNDABS

#Match Gover Leg to FUNDALIST_CRSPIDS
# EINDEX cboard ppill goldenparachute supermajor labylw lachtr
rmvars = ['TICKER', 'CN6', 'CIK', 'DATE1', 'DATE2', "YEAR", 'CONAME', 'EXCH', 'STATE', 'DE_INC', 'GINDEX', 'DUALCLASS',
          'CBOARD', "PPILL", 'GOLDENPARACHUTE', 'SUPERMAJOR', 'LABYLW', 'LACHTR']
kevars = ['TICKER', 'CN6', 'CIK', 'DATE1', 'DATE2', "YEAR", 'CONAME', 'EXCH', 'STATE', 'DE_INC', 'GINDEX', 'DUALCLASS',
          'EINDEX']
eindex = ['CBOARD', "PPILL", 'GOLDENPARACHUTE', 'SUPERMAJOR', 'LABYLW', 'LACHTR']
gover_legacy = gover_legacy[rmvars]

# M = gover_legacy[gover_legacy.TICKER == "AZA"]
gover_legacy['EINDEX'] = gover_legacy[eindex].sum(axis=1)
gover_legacy = gover_legacy[kevars]
gover_legacy['DATE1'] = pd.to_datetime(gover_legacy['DATE1'])
gover_legacy['DATE2'] = pd.to_datetime(gover_legacy['DATE2'])

gover_legacyID2 = pd.merge(gover_legacy, MAINID, left_on=['TICKER'], right_on=['TICCRSP'], how='left')
# gover_legacyID2 = gover_legacyID2.dropna(subset=['LINKENDDT'])
# gover_legacyID2 = gover_legacyID2.astype({'datadate': 'int64', 'LINKDT':'int64', 'LINKENDDT':'int64'})
gover_legacyID2 = gover_legacyID2[(gover_legacyID2.datadate >= gover_legacyID2.DATE1) &
                                  (gover_legacyID2.datadate <= gover_legacyID2.DATE2)]  # GOOD UNEXPECTED
############################
gover_legacyID2.to_csv(os.path.join(data_iss, "gover_legID.gz"), index=False, compression='gzip')

gover_legacyID2 = Functions.create_groups(gover_legacyID2, 'YEAR', "GINDEX", 'GIGRP', grps=2)

############################
# First match gover_legacy to MAINID

# take_over_index create several lags
take_over_index

take_over_index['hostile_index_L1'] = take_over_index.groupby('gvkey')['hostile_index'].shift(1)
take_over_index['hostile_index_L2'] = take_over_index.groupby('gvkey')['hostile_index'].shift(2)
take_over_index['hostile_index_L3'] = take_over_index.groupby('gvkey')['hostile_index'].shift(3)
take_over_index['hostile_index_L4'] = take_over_index.groupby('gvkey')['hostile_index'].shift(4)
#######################
take_over_index.to_csv(os.path.join(data_takeover, 'takeover_index_lags.csv.gz'), index=False, compression='gzip')
#######################

# Execucomp calculate % owned by managers

acomp_m = pd.merge(acomp, FUNDABS[['gvkey', 'fyear', 'csho']],
                   left_on=['GVKEY', 'YEAR'], right_on=['gvkey', 'fyear'], how='left')

# Rank execs by SHROWN_EXCL_OPTS and sum
list_replace = ['SHROWN_EXCL_OPTS', 'OPT_UNEX_EXER_NUM']
# list_drop = ['cmp','dltp', 'dm']
for i in list_replace:
    acomp_m[i].fillna(0, inplace=True)


acomp_m['TOT_SHARES_EXER_OPT'] = acomp_m['SHROWN_EXCL_OPTS'] + acomp_m['OPT_UNEX_EXER_NUM']
acomp_m['TOT_SHARES_OPT_ALL'] = acomp_m['SHROWN_EXCL_OPTS'] + acomp_m['OPT_UNEX_EXER_NUM'] \
                                + acomp_m['OPT_UNEX_UNEXER_NUM']

acomp_m['TOT_SHARES_EXER_OPT_PCT'] = (acomp_m['TOT_SHARES_EXER_OPT']) / (acomp_m['csho']*1000)
acomp_m['TOT_SHARES_OPT_ALL_PCT'] = (acomp_m['TOT_SHARES_OPT_ALL']) / (acomp_m['csho']*1000)
acomp_m['TOT_PCT'] = acomp_m['SHROWN_EXCL_OPTS'] / (acomp_m['csho']*1000)

acomp_m['Rank_A'] = acomp_m.groupby(['GVKEY', 'YEAR'])['SHROWN_EXCL_OPTS'].rank(ascending=False)
acomp_m['Rank_B'] = acomp_m.groupby(['GVKEY', 'YEAR'])['TOT_SHARES_EXER_OPT'].rank(ascending=False)
acomp_m['Rank_C'] = acomp_m.groupby(['GVKEY', 'YEAR'])['TOT_SHARES_OPT_ALL'].rank(ascending=False)

acomp_m['temp_A'] = np.where(acomp_m['Rank_A'] <= 5, 1, 0)
acomp_m['temp_B'] = np.where(acomp_m['Rank_B'] <= 5, 1, 0)
acomp_m['temp_C'] = np.where(acomp_m['Rank_C'] <= 5, 1, 0)

comp_A = pd.pivot_table(acomp_m, values='TOT_PCT', index=['GVKEY', 'YEAR'],
                        columns=['temp_A'], aggfunc=np.sum).reset_index()
comp_B = pd.pivot_table(acomp_m, values='TOT_SHARES_EXER_OPT_PCT', index=['GVKEY', 'YEAR'],
                        columns=['temp_B'], aggfunc=np.sum).reset_index()
comp_C = pd.pivot_table(acomp_m, values='TOT_SHARES_OPT_ALL_PCT', index=['GVKEY', 'YEAR'],
                        columns=['temp_C'], aggfunc=np.sum).reset_index()


comp_A['PCT_EXCL_OPT'] = comp_A[1]
comp_B['PCT_INCL_EX_OPT'] = comp_B[1]
comp_C['PCT_INCL_UEX_OPT'] = comp_C[1]
compensation = pd.merge(comp_A[['GVKEY', 'YEAR', 'PCT_EXCL_OPT']], comp_B[['GVKEY', 'YEAR', 'PCT_INCL_EX_OPT']],
                        left_on=['GVKEY', 'YEAR'], right_on=['GVKEY', 'YEAR'], how='left')

compensation = pd.merge(compensation, comp_C[['GVKEY', 'YEAR', 'PCT_INCL_UEX_OPT']],
                        left_on=['GVKEY', 'YEAR'], right_on=['GVKEY', 'YEAR'], how='left')
compensation[compensation.PCT_EXCL_OPT > 1]
del comp_A
del comp_B
del comp_C

list_variables_WINSOR = ['PCT_EXCL_OPT', 'PCT_INCL_EX_OPT', 'PCT_INCL_UEX_OPT']

compensation = Functions.winsor(compensation, column=list_variables_WINSOR, quantiles=[0.99, 0.01], yearvar='YEAR')

compensation_g = Functions.create_groups(compensation, 'YEAR', "PCT_EXCL_OPT_cut", 'OWN_A', grps=2)
compensation_g = Functions.create_groups(compensation_g, 'YEAR', "PCT_INCL_EX_OPT_cut", 'OWN_B', grps=2)
compensation_g = Functions.create_groups(compensation_g, 'YEAR', "PCT_INCL_UEX_OPT_cut", 'OWN_C', grps=2)

compensation_g.to_csv(os.path.join(data_takeover, 'compensation.csv.gz'), index=False, compression='gzip')

del compensation_g
#OPT_UNEX_EXER_NUM -- Unexercised Exercisable Options
#OPT_UNEX_UNEXER_NUM -- Unexercised Unexercisable Options
#OPT_UNEX_EXER_EST_VAL -- Estimated Value of In-the-Money Unexercised E
#OPT_UNEX_UNEXER_EST_VAL -- Estimated Value Of In-the-Money Unexercised U
# STOCK_UNVEST_NUM -- Restricted Stock Holdings
# STOCK_UNVEST_VAL -- Restricted Stock Holdings ($)
# SHROWN_EXCL_OPTS
# SHROWN_EXCL_OPTS_PCT
c=compensation.groupby(['YEAR'])[['PCT_EXCL_OPT']].quantile(0.5)
compensation.groupby(['YEAR'])[['PCT_EXCL_OPT']].quantile(1)
print(c.loc[1])
c[1]

del COMPUCRSP
del COMPUCRSPIQ
del COMPUCRSPIQCR1
del FUNDASIC
del FUNDASIC_NEWSIC
# match to compustat or CRSP to get total number of shares of the company? we have gvkey, so no need to match, but get
# shares outstanding same fiscal
# Use two measures, one SHROWN_EXCL_OPTS and another SHROWN_EXCL_OPTS plus OPT_UNEX_EXER_NUM (they are in 1000s?)
# COmpustat csho is in millions
# Thomson Reuters
# Institutional Ownership

# M = gover_legacy[gover_legacy.TICKER == "AZA"]

FUNDALIST_CRSPIDS = pd.read_csv(os.path.join(datadirectory, "FUNDALIST_MARCH03.csv.gz")) #, index_col=0)



# TRINST['date'] = TRINST['rdate'].apply(int)
TRINST['date'] = TRINST['rdate'].apply(lambda x: str(x).split('.')[0])

TRINST['date'] = pd.to_datetime(TRINST['date'])
FUNDALIST_CRSPIDS['datadate'] = pd.to_datetime(FUNDALIST_CRSPIDS['datadate'])

FUNDALIST_CRSPIDS['HCNEW'] = FUNDALIST_CRSPIDS['CUSIP'].apply(lambda x: str(x).split('.')[0].zfill(8))
FUNDALIST_CRSPIDS['CNEW'] = FUNDALIST_CRSPIDS['NCUSIP'].apply(lambda x: str(x).split('.')[0].zfill(8))
TRINST['cusip'] = TRINST['cusip'].apply(lambda x: str(x).split('.')[0].zfill(8))



#Convert dates to days because I can't find a better way...
TRINST['temp'] = '19600101'
FUNDALIST_CRSPIDS['temp'] = '19600101'
TRINST['temp'] = pd.to_datetime(TRINST['temp'])
FUNDALIST_CRSPIDS['temp'] = pd.to_datetime(FUNDALIST_CRSPIDS['temp'])
TRINST['tempdays_y'] = (TRINST['date']-TRINST['temp']).dt.days
FUNDALIST_CRSPIDS['tempdays_x'] = (FUNDALIST_CRSPIDS['datadate'] - FUNDALIST_CRSPIDS['temp']).dt.days
# last date on SP CR data base 20170228
#Merge



#merge check back into FUNDALIST_CRSPIDS, check which do not have amatch

FUNDALIST_CRSPIDS['datadate'] = pd.to_datetime(FUNDALIST_CRSPIDS['datadate'])
CHECK['datadate'] = pd.to_datetime(CHECK['datadate'])


CHECKN = pd.merge(FUNDALIST_CRSPIDS[['gvkey', 'datadate', 'fyear', 'EXCHCD', 'COMNAM', 'TICKER', 'CNEW', 'tempdays_x']],
                         TRINST[['cusip', 'InstOwn_Perc', 'date', 'tempdays_y']],
                         left_on=['CNEW'],
                         right_on=['cusip'], how='left')
CHECKN = CHECKN.dropna(subset=['tempdays_y'])


CHECKN['DLR'] = CHECKN['tempdays_x'] - CHECKN['tempdays_y']


CHECKN = CHECKN[CHECKN.DLR >= 0]
CHECKN = CHECKN[CHECKN.DLR <= 360]

CHECKN = CHECKN.sort_values(by=['gvkey', 'datadate', 'DLR'])
CHECKN = CHECKN.drop_duplicates(subset=['gvkey', 'datadate'])

COMPUCRSPIQCRC = pd.merge(FUNDALIST_CRSPIDS[['gvkey', 'datadate', 'fyear', 'EXCHCD', 'COMNAM', 'TICKER']],
                         CHECK1[['gvkey', 'datadate', 'InstOwn_Perc']],
                         left_on=['gvkey', 'datadate'],
                         right_on=['gvkey', 'datadate'], how='left')


COMPUCRSPIQCR = pd.merge(FUNDALIST_CRSPIDS[['gvkey', 'datadate', 'fyear', 'EXCHCD', 'COMNAM', 'TICKER']],
                         CHECKN[['gvkey', 'datadate', 'InstOwn_Perc']],
                         left_on=['gvkey', 'datadate'],
                         right_on=['gvkey', 'datadate'], how='left')

COMPUCRSPIQCR.to_csv(os.path.join(datadirectory, "FUNDALIST_MARCH28INSTOWN.csv.gz"), index=False, compression='gzip')
c = FUNDALIST_CRSPIDS[FUNDALIST_CRSPIDS.gvkey == 1011]
cc = TRINST[TRINST.stkname == 'ACS ENTERPRISES INC']
# Board independence

dir_legacy
check2 = dir_new[dir_new.name == 'BALL CORPORATION']
check3 = dir_legacy[dir_legacy.NAME == 'BALL']
# CRSP has 8 digit CUSIP, dir_new has 9 digit cusip (erase the last one), dir_legacy has 6 digit cusip
# First, count number of directors and independent directors.
#Turn Classification into i = 1, rest = 0

dir_legacy['dirclas'] = np.where(dir_legacy['CLASSIFICATION'] == 'I', 1, 0)
dir_new['dirclas'] = np.where(dir_new['classification'] == 'I', 1, 0)

dir_leg = dir_legacy.groupby(['YEAR', 'CUSIP'])[['dirclas']].mean()
# turn dir_new cusip from 9 to 8
c=FUNDALIST_CRSPIDS[FUNDALIST_CRSPIDS.COMNAM == 'AGILENT TECHNOLOGIES INC']

dir_new['CNEW'] = dir_new['cusip'].str.slice(0, 8)
dir_ne = dir_new.groupby(['year', 'CNEW'])['dirclas'].mean()

dir_leg = dir_leg.reset_index()
dir_ne = dir_ne.reset_index()

#Merge ID name back into dir_leg an

FUNDALIST_CRSPIDS['CNEW6D'] = FUNDALIST_CRSPIDS['CNEW'].str.slice(0, 6)
# For ISS, it seems that it is better to use CRSP CUSIP and not NCUSIP
# COMPUCRSPIQCR2 using CUSIP CRSP
# COMPUCRSPIQCR3 using NCUSIP CRSP

COMPUCRSPIQCR3 = pd.merge(FUNDALIST_CRSPIDS[['gvkey', 'datadate', 'fyear', 'CNEW6D', 'CNEW']],
                         dir_leg[['YEAR', 'CUSIP', 'dirclas']],
                         left_on=['fyear', 'CNEW6D'],
                         right_on=['YEAR', 'CUSIP'], how='left')

COMPUCRSPIQCR3 = pd.merge(COMPUCRSPIQCR3,
                         dir_ne[['year', 'CNEW', 'dirclas']],
                         left_on=['fyear', 'CNEW'],
                         right_on=['year', 'CNEW'], how='left')

#Merge x and y for 2 and 3, then merge 2 and 3 keep combo
where(acomp_m['Rank_A'] <= 5, 1, 0)

COMPUCRSPIQCR2['ind_dir_per'] = np.where(COMPUCRSPIQCR2.dirclas_x.isna(), COMPUCRSPIQCR2.dirclas_y, COMPUCRSPIQCR2.dirclas_x)
COMPUCRSPIQCR3['ind_dir_per'] = np.where(COMPUCRSPIQCR3.dirclas_x.isna(), COMPUCRSPIQCR3.dirclas_y, COMPUCRSPIQCR3.dirclas_x)

C2 = np.where(COMPUCRSPIQCR2.dirclas_x.isna(), COMPUCRSPIQCR2.dirclas_y, COMPUCRSPIQCR2.dirclas_x)
COMPUCRSPIQCR3['ind_dir_per'] = np.where(COMPUCRSPIQCR3.dirclas_x.isna(), COMPUCRSPIQCR3.dirclas_y, COMPUCRSPIQCR3.dirclas_x)


COMPUCRSPIQCR2 = pd.merge(COMPUCRSPIQCR2, COMPUCRSPIQCR3[['gvkey', 'fyear', 'ind_dir_per']],
                          left_on=['gvkey', 'fyear'], right_on=['gvkey', 'fyear'], how='left')
COMPUCRSPIQCR2['ind_dir_per_ISS'] = np.where(COMPUCRSPIQCR2.ind_dir_per_x.isna(),
                                         COMPUCRSPIQCR2.ind_dir_per_y, COMPUCRSPIQCR2.ind_dir_per_x)


### LAST
COMPUCRSPIQCR.to_csv(os.path.join(datadirectory, "FUNDALIST_March29.csv.gz"), index=False, compression='gzip')


# Merge COMPUCRPIQCR with COMPUCRSPIQCR2 Save to file and add to the rerst

COMPUCRSPIQCR = pd.merge(COMPUCRSPIQCR, COMPUCRSPIQCR2[['gvkey', 'fyear', 'ind_dir_per_ISS']],
                          left_on=['gvkey', 'fyear'], right_on=['gvkey', 'fyear'], how='left')
######### CEHCKKING

COMPUCRSPIQCR_small = COMPUCRSPIQCR2.dropna(subset=['ind_dir_per_x'])
COMPUCRSPIQCR_small2 = COMPUCRSPIQCR2.dropna(subset=['ind_dir_per_y'])
COMPUCRSPIQCR_small3 = COMPUCRSPIQCR2.dropna(subset=['ind_dir_per_ISS'])
COMPUCRSPIQCR_small4 = COMPUCRSPIQCR_small3[COMPUCRSPIQCR_small3.ind_dir_per_ISS <= 0.5]
COMPUCRSPIQCR_small4 = COMPUCRSPIQCR_small4[COMPUCRSPIQCR_small4.fyear == 2001]
c = FUNDALIST_CRSPIDS[FUNDALIST_CRSPIDS.gvkey == 24293]
c1 = dir_new[dir_new.ticker == 'MDR']
c1 = dir_new[dir_new.company_id == 96455]

l1 = dir_legacy[dir_legacy.TICKER == 'KLU']

c2 = dir_ne[dir_ne.CNEW == '58003770']
b = board_comp_profile[board_comp_profile.Ticker == 'MDR']
b1 = board_comp[board_comp.CompanyID == 19941]
## Add boardex data



## Add PERMNO AND GVKEY to dir_new and dir_legacy? Add
dir_legacy_small = dir_legacy[['YEAR', 'LEGACY_PPS_ID', 'TICKER', 'CUSIP', 'NAME']]
dir_new_small = dir_new[['company_id', 'cusip', 'ticker', 'year', 'CNEW']]

dir_legacy_small = dir_legacy_small.sort_values(by=['CUSIP', 'LEGACY_PPS_ID'])
dir_new_small = dir_new_small.sort_values(by=['CNEW', 'company_id'])

dir_legacy_small = dir_legacy_small.drop_duplicates(subset=['CUSIP', 'LEGACY_PPS_ID'])
dir_new_small = dir_new_small.drop_duplicates(subset=['CNEW', 'company_id'])

ALLIDFILE = pd.read_csv(os.path.join(datadirectory, "MERGEDIDCR-ALLFEB27.csv.gz"))

ALLIDFILE['CAPIQ_companyid'] = ALLIDFILE['companyid']

COMPUCRSPIQCR3 = pd.merge(FUNDALIST_CRSPIDS[['gvkey', 'datadate', 'fyear', 'CNEW6D', 'CNEW']],
                         dir_leg[['YEAR', 'CUSIP', 'dirclas']],
                         left_on=['fyear', 'CNEW6D'],
                         right_on=['YEAR', 'CUSIP'], how='left')


## Dual class data from Metrick et al and add



# dir legacy

dir_legacy = pd.read_csv(os.path.join(data_dir, "ISS - Directors Legacy Data Request.gz"), sep='\t',
                           encoding="ISO-8859-1")

dir_legacy['dirclas'] = np.where(dir_legacy['CLASSIFICATION'] == 'I', 1, 0)
dir_legacy = dir_legacy.sort_values(by=['YEAR', 'CUSIP'])
dir_legacy_s = dir_legacy[dir_legacy.YEAR == 2001]
dir_leg = dir_legacy.groupby(['YEAR', 'CUSIP'])[['dirclas']].mean()
dir_leg = dir_legacy_s.groupby(['CUSIP'])[['dirclas']].mean()
dir_leg['b'] = np.where(dir_leg['dirclas']<=0.5, 1, 0)
dir_leg['dirclas'].mean()
dir_leg['dirclas'].median()
dir_leg['b'].mean()


# Check trace enhanced
data_trace = os.path.join(data_all, 'TRACE') #gvkey dataset
data_trace = pd.read_csv(os.path.join(data_trace, "Enhanced.gz"), sep='\t',
                           encoding="ISO-8859-1", nrows=1000) #ticker CUSIP 6 igit and CIK