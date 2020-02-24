# First, match COMPUSTAT, CRSP and CAPITALIQ make sure to add CUSIP and CIK to the table to merge with other datasets
# Calculate debt measures
# Compustat:
# SUB = dcvsub + ds
# SBN = DD+DN
# BD = DLTO - CMP
# CL = CL
# CMP = CMP
# short = dlc-dd1
# Change to reculaculate HH1 indexes
import pandas as pd
import numpy as np
import Functions
import importlib
import os
importlib.reload(Functions)

datadirectory = os.path.join(os.getcwd(), 'data')

FUNDADEBT = 0
FUNDACMP = 0

FUNDADEBT = pd.merge(FUNDADEBT,
                     FUNDACMP[['gvkey', 'datadate', 'cmp']],
                     left_on=['gvkey', 'datadate'],
                     right_on=['gvkey', 'datadate'], how='left')


list_replace = ['cmp', 'dltp', 'dm']
list_replace = ['dltt', 'dm', 'cmp', 'dcvsub', 'ds', 'dd', 'dn', 'dlto', 'dlc', 'dd1', 'dclo', 'dcvt', 'dltp', 'dcvsr']
# list_drop = ['cmp','dltp', 'dm']

for i in list_replace:
    FUNDADEBT[i].fillna(0, inplace=True)

F_check = FUNDADEBT[FUNDADEBT.gvkey == 1239]

# variables to modify
FUNDADEBT['SUBNOTCONV_C'] = FUNDADEBT['ds']
FUNDADEBT['SUBCONV_C'] = FUNDADEBT['dcvsub']
FUNDADEBT['CONV_C'] = FUNDADEBT['dcvsr']
FUNDADEBT['DD_C'] = FUNDADEBT['dd']
FUNDADEBT['DN_C'] = FUNDADEBT['dn']
FUNDADEBT['DLTO'] = FUNDADEBT['dlto']
FUNDADEBT['CMP'] = FUNDADEBT['cmp']
FUNDADEBT['CL_C'] = FUNDADEBT['dclo']

# FUNDADEBTSSSS = FUNDADEBT[FUNDADEBT.gvkey == 1084]

list_vars = ['SUBNOTCONV_C', 'SUBCONV_C', 'CONV_C', 'DD_C', 'DN_C', 'DLTO', 'CMP', 'CL_C']

FUNDADEBT['CHECK_DEBT'] = FUNDADEBT[list_vars].sum(axis=1)
FUNDADEBT['CCC'] = FUNDADEBT['dltt'] - FUNDADEBT['CHECK_DEBT']
FUNDADEBT['NP_Exact'] = np.where((FUNDADEBT['CCC'] >= -0.001001) & (FUNDADEBT['CCC'] <= 0.001001), 1, 0)
FUNDADEBT['NP_UNDER'] = np.where((FUNDADEBT['CCC'] > 0.001001), 1, 0)
FUNDADEBT['NP_OVER'] = np.where((FUNDADEBT['CCC'] < -0.001001), 1, 0)
FUNDADEBT['COUNT'] = np.where((FUNDADEBT['dltt'] > 0) & (FUNDADEBT['CHECK_DEBT'] > 0), 1, 0)
FUNDADEBT['CHECK_DEBT'] = FUNDADEBT[list_vars].sum(axis=1)
# FUNDADEBT['CHECK_DEBT2'] = FUNDADEBT[list_vars].sum(axis=1)
# FUNDADEBT['CCC2'] = FUNDADEBT['dltt'] + FUNDADEBT['dd1'] - FUNDADEBT['CHECK_DEBT2']
# FUNDADEBT['CCC3'] = FUNDADEBT['dltt'] - FUNDADEBT['dd1'] - FUNDADEBT['CHECK_DEBT2']

FUNDADEBT = Functions.pct_calculator(list_vars, 'CHECK_DEBT', 'PCT', FUNDADEBT)

FUNDADEBTS = FUNDADEBT[(FUNDADEBT.gvkey == 33152)]

list_varsp = ['SUBNOTCONV_CPCT', 'SUBCONV_CPCT', 'CONV_CPCT', 'DD_CPCT', 'DN_CPCT', 'DLTOPCT', 'CMPPCT', 'CL_CPCT']
for i in list_varsp:
    FUNDADEBT[i].fillna(0, inplace=True)
# FUNDADEBTSSSSS = FUNDADEBT[FUNDADEBT.gvkey == 1084]

FUNDADEBT['TOTALDEBT_C_U'] = FUNDADEBT['SUBNOTCONV_C'] + FUNDADEBT['SUBCONV_C'] +\
                         FUNDADEBT['CONV_C'] + FUNDADEBT['DD_C'] + FUNDADEBT['DN_C'] + FUNDADEBT['BD_C'] +\
                         FUNDADEBT['CL_C'] + FUNDADEBT['SHORT_C']

FUNDADEBT = Functions.hhi_calculator(['SUBNOTCONV_C', 'SUBCONV_C', 'CONV_C', 'DD_C', 'DN_C', 'BD_C', 'CL_C','SHORT_C'],
                                     'TOTALDEBT_C_U', 'HH1U', FUNDADEBT)
FUNDADEBT = Functions.hhi_calculator(['SUB_C', 'SBN_C', 'BD_C', 'CL_C', 'SHORT_C'], 'TOTALDEBT_C_U', 'HH2U', FUNDADEBT)

FUNDADEBT = Functions.adj_dd1(FUNDADEBT, list_vars)
# FUNDADEBT = Functions.adj_dd1_old(FUNDADEBT, list_vars, conditions=['NP_UNDER', 'NP_OVER', 'dd1'])
# FUNDADEBTSSSSS = FUNDADEBT[FUNDADEBT.gvkey == 1084]



# FUNDADEBTSSSSSS = FUNDADEBT[FUNDADEBT.gvkey == 1084]
# FUNDADEBTS2 = FUNDADEBT[(FUNDADEBT.gvkey == 33152)]

FUNDADEBT['CHECK_DEBT2'] = FUNDADEBT[list_vars].sum(axis=1)
FUNDADEBT['CCC2'] = FUNDADEBT['dltt'] + FUNDADEBT['dd1'] - FUNDADEBT['CHECK_DEBT2']

FUNDADEBT['NPOU_Exact'] = np.where((FUNDADEBT['CCC2'] >= -0.00101)
                                       & (FUNDADEBT['CCC2'] <= 0.00101), 1, 0)

FUNDADEBT['DEBTSUM_ERR'] = FUNDADEBT['CCC2']/FUNDADEBT['dltt']

FUNDADEBT['KEEP_E'] = np.where((FUNDADEBT['DEBTSUM_ERR'] >= -0.1)
                                       & (FUNDADEBT['DEBTSUM_ERR'] <= 0.1), 1, 0)



FUNDADEBT = FUNDADEBT.drop(columns=['CCC2', 'CHECK_DEBT2', 'CHECK_DEBT'])

# handle situations where dltt = 0, but dd1 > 0

list_varsp = ['SUBNOTCONV_CPCT', 'SUBCONV_CPCT', 'CONV_CPCT', 'DD_CPCT', 'DN_CPCT', 'DLTOPCT', 'CMPPCT', 'CL_CPCT']

FUNDADEBT['TOTALDEBT_C'] = FUNDADEBT['dltt'] + FUNDADEBT['dlc']
FUNDADEBT['SUB_C'] = FUNDADEBT['SUBNOTCONV_C'] + FUNDADEBT['SUBCONV_C']
FUNDADEBT['SBN_C'] = FUNDADEBT['DD_C'] + FUNDADEBT['DN_C'] + FUNDADEBT['CONV_C']
FUNDADEBT['BD_C'] = FUNDADEBT['DLTO'] #later after adjustment
# FUNDADEBT['CL_C'] = FUNDADEBT['CL_C']
FUNDADEBT['SHORT_C'] = FUNDADEBT['dlc'] - FUNDADEBT['dd1'] + FUNDADEBT['CMP']
FUNDADEBT['OTHER_C'] = FUNDADEBT['TOTALDEBT_C'] - FUNDADEBT['SUB_C'] - FUNDADEBT['SBN_C'] - FUNDADEBT['BD_C'] \
                       - FUNDADEBT['CL_C'] - FUNDADEBT['SHORT_C'] - FUNDADEBT['cmp']

FUNDADEBT['BDB_C'] = FUNDADEBT['BD_C'] + FUNDADEBT['SHORT_C']

#NEW

FUNDADEBT['TOTALDEBT_C_2'] = FUNDADEBT['SUBNOTCONV_C'] + FUNDADEBT['SUBCONV_C'] +\
                         FUNDADEBT['CONV_C'] + FUNDADEBT['DD_C'] + FUNDADEBT['DN_C'] + FUNDADEBT['BD_C'] +\
                         FUNDADEBT['CL_C'] + FUNDADEBT['SHORT_C'] #  + FUNDADEBT['cmp']

# FUNDADEBTSSSS = FUNDADEBT[FUNDADEBT.gvkey == 1084]

FUNDADEBT = Functions.hhi_calculator(['SUBNOTCONV_C', 'SUBCONV_C', 'CONV_C', 'DD_C', 'DN_C', 'BD_C', 'CL_C','SHORT_C'],
                                     'TOTALDEBT_C_2', 'HH1', FUNDADEBT)
FUNDADEBT = Functions.hhi_calculator(['SUB_C', 'SBN_C', 'BD_C', 'CL_C', 'SHORT_C'], 'TOTALDEBT_C_2', 'HH2', FUNDADEBT)
# FUNDADEBTSSSS = FUNDADEBT[FUNDADEBT.gvkey == 1048]
# FUNDADEBTSSSSS = FUNDADEBT[FUNDADEBT.KEEP_E == 0]


list_sum2 = ['SUB_C','SBN_C','BD_C', 'CL_C', 'SHORT_C']
FUNDADEBT = Functions.pct_calculator(list_sum2, 'TOTALDEBT_C_2', 'PCT', FUNDADEBT)
list_sum2 = ['SUBNOTCONV_C','SUBCONV_C','CONV_C','DD_C', 'DN_C', 'BD_C', 'CL_C', 'SHORT_C']
FUNDADEBT = Functions.pct_calculator(list_sum2, 'TOTALDEBT_C_2', 'PCT', FUNDADEBT)


FUNDADEBT.to_csv(os.path.join(datadirectory, "fundadebtprocessedJAN30.csv.gz"), index=False, compression='gzip')




FUNDADEBTS = FUNDADEBT[FUNDADEBT.gvkey == 1239]

FUNDADEBT = FUNDADEBT.drop(columns=['rcc','last_1','CHECK_DEBT','CCC'])

FUNDADEBTSSS = FUNDADEBT[FUNDADEBT.dd1 > 0]

FUNDADEBTSSS['CHECKC'] = np.where((FUNDADEBTSSS['dd1'] > 0) & (FUNDADEBTSSS['dltt'] == 0), 1, 0)
FUNDADEBTSSS = FUNDADEBTSSS[FUNDADEBTSSS.CHECKC == 1]


FUNDADEBT['CHECK_DEBT'] = FUNDADEBT['dd'] + FUNDADEBT['dn'] + FUNDADEBT['dcvt'] + \
                          FUNDADEBT['dlto'] + FUNDADEBT['ds'] + FUNDADEBT['dclo']



FUNDADEBT['CCC'] = FUNDADEBT['dltt'] - FUNDADEBT['CHECK_DEBT']

FUNDADEBT['CCC'] = FUNDADEBT['TOTALDEBT_C'] - FUNDADEBT['TOTALDEBT_C_2']
#FUNDADEBT['PCCC'] = (FUNDADEBT['dltt'] - FUNDADEBT['CHECK_DEBT'])/FUNDADEBT['dltt']

FUNDADEBT['N_dd1_lt'] = np.where((FUNDADEBT['dltt'] == 0) & (FUNDADEBT['dd1'] > 0), 1, 0)
FUNDADEBT['NP_Exact'] = np.where((FUNDADEBT['CCC'] >= -0.001001) & (FUNDADEBT['CCC'] <= 0.001001), 1, 0)
FUNDADEBT['NP_UNDER'] = np.where((FUNDADEBT['CCC'] > 0.001001), 1, 0)
FUNDADEBT['NP_OVER'] = np.where((FUNDADEBT['CCC'] < -0.001001), 1, 0)

FUNDADEBT['rcc'] = np.where((FUNDADEBT['NP_UNDER'] == 1), FUNDADEBT['CCC'] - FUNDADEBT['dd1'],
                               FUNDADEBT['CCC'] + FUNDADEBT['dd1'])

FUNDADEBT['NPOU_Exact'] = np.where((FUNDADEBT['rcc'] >= -0.00101)
                                       & (FUNDADEBT['rcc'] <= 0.00101), 1, 0)

FUNDADEBT['DEBTSUM_ERR'] = FUNDADEBT['CCC']/FUNDADEBT['TOTALDEBT_C']

FUNDADEBT['KEEP_1'] = np.where((FUNDADEBT['DEBTSUM_ERR'] >= -0.1)
                                       & (FUNDADEBT['DEBTSUM_ERR'] <= 0.1), 1, 0)


FUNDADEBT['KEEP_2'] = np.where((FUNDADEBT['NP_Exact'] == 1) | (FUNDADEBT['NPOU_Exact'] == 1) | (FUNDADEBT['KEEP_1'] == 1)
                                  , 1, 0) #most important
FUNDADEBT['KEEP_3'] = np.where((FUNDADEBT['KEEP_2'] == 1)
                                       & (FUNDADEBT['NPOU_Exact'] == 0), 1, 0)



FUNDADEBT = Functions.hhi_calculator(['SUBNOTCONV_C','SUBCONV_C','CONV_C', 'DD_C', 'DN_C', 'BD_C', 'CL_C','SHORT_C'],
                                     'TOTALDEBT_C_2', 'HH1', FUNDADEBT)

FUNDADEBT = Functions.hhi_calculator(['SUB_C','SBN_C','BD_C', 'CL_C', 'SHORT_C'], 'TOTALDEBT_C_2', 'HH2', FUNDADEBT)

FUNDADEBT = Functions.hhi_calculator(['SUB_C','SBN_C','BD_C', 'CL_C', 'SHORT_C', 'OTHER_C'], 'TOTALDEBT_C', 'HH2B', FUNDADEBT)
FUNDADEBT = Functions.hhi_calculator(['SUBNOTCONV_C','SUBCONV_C','CONV_C', 'DD_C', 'DN_C', 'BD_C', 'CL_C','SHORT_C', 'OTHER_C'],
                                     'TOTALDEBT_C', 'HH1B', FUNDADEBT)



list_sum2 = ['SUB_C','SBN_C','BD_C', 'CL_C', 'SHORT_C']
FUNDADEBT = Functions.pct_calculator(list_sum2, 'TOTALDEBT_C_2', 'PCT', FUNDADEBT)
list_sum2 = ['SUBNOTCONV_C','SUBCONV_C','CONV_C','DD_C', 'DN_C', 'BD_C', 'CL_C', 'SHORT_C']
FUNDADEBT = Functions.pct_calculator(list_sum2, 'TOTALDEBT_C_2', 'PCT', FUNDADEBT)


FUNDADEBT.to_csv(os.path.join(datadirectory, "fundadebtprocessedDEC16.csv.gz"), index=False, compression='gzip')

FUNDADEBT = FUNDADEBT.drop(columns=['rcc','last_1','CHECK_DEBT','CCC'])




























#COmpustat substracts dd1 from each component of long term debt. Add it back

list_sum1 = ['SUB_C','SBN_C','BD_C', 'CL_C', 'SHORT_C', 'cmp']
list_sum2 = ['SUB_C','SBN_C','BD_C', 'CL_C', 'SHORT_C', 'cmp', 'OTHER_C']


FUNDADEBT['CHECK_C'] = (FUNDADEBT[list_sum2].sum(axis=1))/FUNDADEBT['TOTALDEBT_C']
FUNDADEBT['CHECK_CO'] = FUNDADEBT[list_sum1].sum(axis=1)/FUNDADEBT['TOTALDEBT_C']
FUNDADEBT['CHECK_2'] = FUNDADEBT[list_sum1].sum(axis=1) - FUNDADEBT['TOTALDEBT_C'] + FUNDADEBT['dd1']
FUNDADEBT['CHECK_3'] = np.where(FUNDADEBT['CHECK_2'].round(1) == 0,1,0) \
                        * np.where(FUNDADEBT['CHECK_CO'] == 1, 0, 1)

# Adjustments, check sTotal_debt - sum total
FUNDADEBT['sum_temp'] = FUNDADEBT[['SUB_C', 'SBN_C', 'BD_C', 'CL_C', 'cmp']].sum(axis=1)
FUNDADEBT = Functions.pct_calculator(list_sum1, 'sum_temp', 'TEMP', FUNDADEBT)

FUNDADEBT['SUB_C'] = FUNDADEBT['SUB_C'] + FUNDADEBT['dd1']*FUNDADEBT['SUB_CTEMP']*FUNDADEBT['CHECK_3']
FUNDADEBT['SBN_C'] = FUNDADEBT['SBN_C'] + FUNDADEBT['dd1']*FUNDADEBT['SBN_CTEMP']*FUNDADEBT['CHECK_3']
FUNDADEBT['BD_C'] = FUNDADEBT['BD_C'] + FUNDADEBT['dd1']*FUNDADEBT['BD_CTEMP']*FUNDADEBT['CHECK_3']
FUNDADEBT['CL_C'] = FUNDADEBT['CL_C'] + FUNDADEBT['dd1']*FUNDADEBT['CL_CTEMP']*FUNDADEBT['CHECK_3']
FUNDADEBT['cmp'] = FUNDADEBT['cmp'] + FUNDADEBT['dd1']*FUNDADEBT['cmpTEMP']*FUNDADEBT['CHECK_3']

FUNDADEBT['CHECK_4'] = (FUNDADEBT[list_sum1].sum(axis=1))/FUNDADEBT['TOTALDEBT_C']

FUNDADEBT['OTHERA_C'] = FUNDADEBT['TOTALDEBT_C'] - FUNDADEBT['SUB_C'] - FUNDADEBT['SBN_C'] - FUNDADEBT['BD_C'] \
                       - FUNDADEBT['CL_C'] - FUNDADEBT['SHORT_C'] - FUNDADEBT['cmp']

#Redo but with new measures
list_sum1 = ['SUBNOTCONV_C','SUBCONV_C','CONV_C','DD_C', 'DN_C', 'BD_C', 'CL_C', 'SHORT_C', 'cmp']
list_sum2 = ['SUBNOTCONV_C','SUBCONV_C','CONV_C','DD_C', 'DN_C', 'BD_C', 'CL_C', 'SHORT_C', 'cmp', 'OTHER_C']

FUNDADEBT['CHECK_C'] = (FUNDADEBT[list_sum2].sum(axis=1))/FUNDADEBT['TOTALDEBT_C']
FUNDADEBT['CHECK_CO'] = FUNDADEBT[list_sum1].sum(axis=1)/FUNDADEBT['TOTALDEBT_C']
FUNDADEBT['CHECK_2'] = FUNDADEBT[list_sum1].sum(axis=1) - FUNDADEBT['TOTALDEBT_C'] + FUNDADEBT['dd1']
FUNDADEBT['CHECK_3'] = np.where(FUNDADEBT['CHECK_2'].round(1) == 0,1,0) \
                        * np.where(FUNDADEBT['CHECK_CO'] == 1, 0, 1)

# Adjustments, check sTotal_debt - sum total
FUNDADEBT['sum_temp'] = FUNDADEBT[['SUBNOTCONV_C','SUBCONV_C','CONV_C','DD_C', 'DN_C', 'BD_C', 'CL_C', 'cmp']].sum(axis=1)
FUNDADEBT = Functions.pct_calculator(list_sum1, 'sum_temp', 'TEMP', FUNDADEBT)

FUNDADEBT['SUBNOTCONV_C'] = FUNDADEBT['SUBNOTCONV_C'] + FUNDADEBT['dd1']*FUNDADEBT['SUBNOTCONV_CTEMP']*FUNDADEBT['CHECK_3']
FUNDADEBT['SUBCONV_C'] = FUNDADEBT['SUBCONV_C'] + FUNDADEBT['dd1']*FUNDADEBT['SUBCONV_CTEMP']*FUNDADEBT['CHECK_3']
FUNDADEBT['CONV_C'] = FUNDADEBT['CONV_C'] + FUNDADEBT['dd1']*FUNDADEBT['CONV_CTEMP']*FUNDADEBT['CHECK_3']
FUNDADEBT['DD_C'] = FUNDADEBT['DD_C'] + FUNDADEBT['dd1']*FUNDADEBT['DD_CTEMP']*FUNDADEBT['CHECK_3']
FUNDADEBT['DN_C'] = FUNDADEBT['DN_C'] + FUNDADEBT['dd1']*FUNDADEBT['DN_CTEMP']*FUNDADEBT['CHECK_3']
FUNDADEBT['BD_C'] = FUNDADEBT['BD_C'] + FUNDADEBT['dd1']*FUNDADEBT['BD_CTEMP']*FUNDADEBT['CHECK_3']
FUNDADEBT['CL_C'] = FUNDADEBT['CL_C'] + FUNDADEBT['dd1']*FUNDADEBT['CL_CTEMP']*FUNDADEBT['CHECK_3']
FUNDADEBT['cmp'] = FUNDADEBT['cmp'] + FUNDADEBT['dd1']*FUNDADEBT['cmpTEMP']*FUNDADEBT['CHECK_3']

FUNDADEBT['CHECK_4'] = (FUNDADEBT[list_sum1].sum(axis=1))/FUNDADEBT['TOTALDEBT_C']

FUNDADEBT['OTHERA2_C'] = FUNDADEBT['TOTALDEBT_C'] - FUNDADEBT['SUBNOTCONV_C'] - FUNDADEBT['SUBCONV_C'] -\
                         FUNDADEBT['CONV_C'] - FUNDADEBT['DD_C'] - FUNDADEBT['DN_C'] - FUNDADEBT['BD_C'] -\
                         FUNDADEBT['CL_C'] - FUNDADEBT['SHORT_C'] - FUNDADEBT['cmp']



#######################
#Calculate percentages#
#######################
#FUNDADEBT = Functions.pct_calculator(['SUB_C','SBN_C','BD_C', 'CL_C', 'cmp','OTHERA_C'], 'TOTALDEBT_C', 'PCT', FUNDADEBT)
FUNDADEBT = Functions.hhi_calculator(['SUB_C','SBN_C','BD_C', 'CL_C', 'SHORT_C', 'cmp','OTHERA_C'], 'TOTALDEBT_C', 'HH1_C', FUNDADEBT)
FUNDADEBT = Functions.hhi_calculator(['SUB_C','SBN_C','BD_C', 'CL_C', 'SHORT_C', 'OTHERA_C'], 'TOTALDEBT_C', 'HH2_C', FUNDADEBT)
FUNDADEBT = Functions.hhi_calculator(['SUB_C','SBN_C','BDB_C', 'CL_C', 'OTHERA_C'], 'TOTALDEBT_C', 'HH3_C', FUNDADEBT)
FUNDADEBT = Functions.hhi_calculator(['SUBNOTCONV_C','SUBCONV_C','CONV_C', 'DD_C', 'DN_C', 'BD_C', 'CL_C','SHORT_C',
                                      'OTHERA2_C'],'TOTALDEBT_C', 'HH4_C', FUNDADEBT)

#New percentages

list_sum2 = ['SUB_C','SBN_C','BD_C', 'CL_C', 'SHORT_C', 'cmp', 'OTHERA_C']
FUNDADEBT = Functions.pct_calculator(list_sum2, 'TOTALDEBT_C', 'PCT', FUNDADEBT)
list_sum2 = ['SUBNOTCONV_C','SUBCONV_C','CONV_C','DD_C', 'DN_C', 'BD_C', 'CL_C', 'SHORT_C', 'cmp', 'OTHERA2_C']
FUNDADEBT = Functions.pct_calculator(list_sum2, 'TOTALDEBT_C', 'PCT', FUNDADEBT)


####################################
###END HERE########################
###################################
#################################





















