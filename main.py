import datetime, csv
#from sas7bdat import SAS7BDAT
import pandas as pd
import pandasql as ps
import numpy as np
import gzip, os, csv
import datetime
import Functions
import importlib

datadirectory = os.path.join(os.getcwd(), 'data')

importlib.reload(Functions)

FUNDADEBT = pd.read_csv(os.path.join(datadirectory, "FUNDADEBT19502018.gz"), sep='\t')
FUNDADEBT['datadate'] = pd.to_datetime(FUNDADEBT['datadate'], format='%Y%m%d')
FUNDADEBT = FUNDADEBT[FUNDADEBT.indfmt == 'INDL']

FUNDADEBT = FUNDADEBT.loc[:, ~FUNDADEBT.columns.str.endswith('_fn')]
FUNDADEBTS = FUNDADEBT.loc[:,~FUNDADEBT.columns.str.endswith('_dc')]

FUNDACMP = pd.read_csv(os.path.join(datadirectory, "fundaCMP7019.gz"), sep='\t')
FUNDACMP = FUNDACMP[['gvkey' ,'datadate' ,'cmp']].dropna(subset=['cmp'])
FUNDACMP['datadate'] = pd.to_datetime(FUNDACMP['datadate'], format='%Y%m%d')

FUNDABS = pd.read_csv(os.path.join(datadirectory, "FUNDAMAINBS5018.gz"), sep='\t')
FUNDABS2 = FUNDABS.loc[:, ~FUNDABS.columns.str.endswith('_dc')]
FUNDABS2 = FUNDABS2.loc[:, ~FUNDABS2.columns.str.endswith('_fn')]
FUNDABS2['datadate'] = pd.to_datetime(FUNDABS2['datadate'], format='%Y%m%d')
FUNDABS = FUNDABS2
del FUNDABS2

FUNDAP = pd.read_csv(os.path.join(datadirectory, "APWCAP5018.gz"), sep='\t')
FUNDAP['datadate'] = pd.to_datetime(FUNDABS['datadate'], format='%Y%m%d')

#FUNDAMAINID = pd.read_csv(os.path.join(datadirectory, "fundamainID7019.txt.gz"), sep='\t')
FUNDAMAINID = pd.read_csv(os.path.join(datadirectory, "FUNDAMAINID5018.gz"), sep='\t')

FUNDAMAINID = FUNDAMAINID[FUNDAMAINID.indfmt == 'INDL']

CRSPLINK = pd.read_csv(os.path.join(datadirectory, "CRSPLINK.gz"), sep='\t')
CAPIQID = pd.read_csv(os.path.join(datadirectory, "Capital IQ Identifier.txt"), sep='\t')

CRSPM = pd.read_csv(os.path.join(datadirectory, "CRSPMONTHLY5018.gz"), sep='\t')
CRSPM['date'] = pd.to_datetime(CRSPM['date'], format='%Y%m%d')

SPCR = pd.read_csv(os.path.join(datadirectory, "cpstCRN.txt"), sep='\t')
SPCR.drop(columns=['tic'])
SPCRNONA = SPCR.drop(columns=['tic']).dropna(subset=['splticrm'])

CRSPM["COMNAM"]

CRSPM.loc[CRSPM['COMNAM'] == 'A P L CORP']








##############OLD STUF






# Adjustments, check sTotal_debt - sum total

list_sum1 = ['SUB_C','SBN_C','BD_C', 'CL_C', 'SHORT_C']


FUNDADEBT['CHECK_C'] = (FUNDADEBT[list_sum2].sum(axis=1))/FUNDADEBT['TOTALDEBT_C']
FUNDADEBT['CHECK_CO'] = FUNDADEBT[list_sum1].sum(axis=1)/FUNDADEBT['TOTALDEBT_C']
FUNDADEBT['CHECK_2'] = FUNDADEBT[list_sum1].sum(axis=1) - FUNDADEBT['TOTALDEBT_C'] + FUNDADEBT['dd1']

#FUNDADEBT['CHECK_3'] = np.where(FUNDADEBT['CHECK_2'].round(1) == 0,1,0) \
                        #* np.where(FUNDADEBT['CHECK_CO'] == 1, 0, 1)


FUNDADEBT['sum_temp'] = FUNDADEBT[['SUB_C','SBN_C','BD_C', 'CL_C', 'cmp']].sum(axis=1)
FUNDADEBT = Functions.pct_calculator(list_sum1, 'sum_temp', 'TEMP', FUNDADEBT)


FUNDADEBT['SUB_C'] = FUNDADEBT['SUB_C'] + FUNDADEBT['dd1']*FUNDADEBT['SUB_CTEMP']*FUNDADEBT['CHECK_3']
FUNDADEBT['SBN_C'] = FUNDADEBT['SBN_C'] + FUNDADEBT['dd1']*FUNDADEBT['SBN_CTEMP']*FUNDADEBT['CHECK_3']
FUNDADEBT['BD_C'] = FUNDADEBT['BD_C'] + FUNDADEBT['dd1']*FUNDADEBT['BD_CTEMP']*FUNDADEBT['CHECK_3']
FUNDADEBT['CL_C'] = FUNDADEBT['CL_C'] + FUNDADEBT['dd1']*FUNDADEBT['CL_CTEMP']*FUNDADEBT['CHECK_3']
FUNDADEBT['cmp'] = FUNDADEBT['cmp'] + FUNDADEBT['dd1']*FUNDADEBT['cmpTEMP']*FUNDADEBT['CHECK_3']

#FUNDADEBT['CHECK_4'] = (FUNDADEBT[list_sum1].sum(axis=1))/FUNDADEBT['TOTALDEBT_C']
FUNDADEBT['OTHERA_C'] = FUNDADEBT['TOTALDEBT_C'] - FUNDADEBT['SUB_C'] - FUNDADEBT['SBN_C'] - FUNDADEBT['BD_C'] \
                       #- FUNDADEBT['CL_C'] - FUNDADEBT['SHORT_C'] - FUNDADEBT['cmp']

#Redo but with new measures
list_sum1 = ['SUBNOTCONV_C','SUBCONV_C','CONV_C','DD_C', 'DN_C', 'BD_C', 'CL_C', 'SHORT_C', 'cmp']
list_sum2 = ['SUBNOTCONV_C','SUBCONV_C','CONV_C','DD_C', 'DN_C', 'BD_C', 'CL_C', 'SHORT_C', 'cmp', 'OTHER_C']

#FUNDADEBT['CHECK_C'] = (FUNDADEBT[list_sum2].sum(axis=1))/FUNDADEBT['TOTALDEBT_C']
FUNDADEBT['CHECK_CO'] = FUNDADEBT[list_sum1].sum(axis=1)/FUNDADEBT['TOTALDEBT_C']
FUNDADEBT['CHECK_2'] = FUNDADEBT[list_sum1].sum(axis=1) - FUNDADEBT['TOTALDEBT_C'] + FUNDADEBT['dd1']
FUNDADEBT['CHECK_3'] = np.where(FUNDADEBT['CHECK_2'].round(1) == 0,1,0) \
                        * np.where(FUNDADEBT['CHECK_CO'] == 1, 0, 1)

# Adjustments, check sTotal_debt - sum total
FUNDADEBT['sum_temp'] = FUNDADEBT[['SUBNOTCONV_C','SUBCONV_C','CONV_C','DD_C', 'DN_C', 'BD_C', 'CL_C', 'cmp']].sum(axis=1)
FUNDADEBT = Functions.pct_calculator(list_sum1, 'sum_temp', 'TEMP', FUNDADEBT)

FUNDADEBT['SUBNOTCONV_CA1'] = FUNDADEBT['SUBNOTCONV_C'] + FUNDADEBT['dd1']*FUNDADEBT['SUBNOTCONV_CTEMP']*FUNDADEBT['CHECK_3']
FUNDADEBT['SUBCONV_CA1'] = FUNDADEBT['SUBCONV_C'] + FUNDADEBT['dd1']*FUNDADEBT['SUBCONV_CTEMP']*FUNDADEBT['CHECK_3']
FUNDADEBT['CONV_CA1'] = FUNDADEBT['CONV_C'] + FUNDADEBT['dd1']*FUNDADEBT['CONV_CTEMP']*FUNDADEBT['CHECK_3']
FUNDADEBT['DD_CA1'] = FUNDADEBT['DD_C'] + FUNDADEBT['dd1']*FUNDADEBT['DD_CTEMP']*FUNDADEBT['CHECK_3']
FUNDADEBT['DN_CA1'] = FUNDADEBT['DN_C'] + FUNDADEBT['dd1']*FUNDADEBT['DN_CTEMP']*FUNDADEBT['CHECK_3']
FUNDADEBT['BD_CA1'] = FUNDADEBT['BD_C'] + FUNDADEBT['dd1']*FUNDADEBT['BD_CTEMP']*FUNDADEBT['CHECK_3']
FUNDADEBT['CL_CA1'] = FUNDADEBT['CL_C'] + FUNDADEBT['dd1']*FUNDADEBT['CL_CTEMP']*FUNDADEBT['CHECK_3']
FUNDADEBT['cmp_CA1'] = FUNDADEBT['cmp'] + FUNDADEBT['dd1']*FUNDADEBT['cmpTEMP']*FUNDADEBT['CHECK_3']

FUNDADEBT['CHECK_4'] = (FUNDADEBT[list_sum1].sum(axis=1))/FUNDADEBT['TOTALDEBT_C']

FUNDADEBT['OTHERA2_C'] = FUNDADEBT['TOTALDEBT_C'] - FUNDADEBT['SUBNOTCONV_CA1'] - FUNDADEBT['SUBCONV_CA1'] -\
                         FUNDADEBT['CONV_CA1'] - FUNDADEBT['DD_CA1'] - FUNDADEBT['DN_CA1'] - FUNDADEBT['BD_CA1'] -\
                         FUNDADEBT['CL_CA1'] - FUNDADEBT['SHORT_CA1'] - FUNDADEBT['cmp_CA1']




FUNDADEBT = Functions.hhi_calculator(['SUB_C','SBN_C','BD_C', 'CL_C', 'SHORT_C', 'OTHER_C'], 'TOTALDEBT_C', 'HH2C', FUNDADEBT)
FUNDADEBT = Functions.hhi_calculator(['SUBNOTCONV_C','SUBCONV_C','CONV_C', 'DD_C', 'DN_C', 'BD_C', 'CL_C','SHORT_C', 'OTHER_C'],
                                     'TOTALDEBT_C', 'HH1C', FUNDADEBT)



#######################
#Calculate percentages#
#######################
#FUNDADEBT = Functions.pct_calculator(['SUB_C','SBN_C','BD_C', 'CL_C', 'cmp','OTHERA_C'], 'TOTALDEBT_C', 'PCT', FUNDADEBT)
#FUNDADEBT = Functions.hhi_calculator(['SUB_C','SBN_C','BD_C', 'CL_C', 'SHORT_C', 'cmp','OTHERA_C'], 'TOTALDEBT_C', 'HH1_C', FUNDADEBT)
#FUNDADEBT = Functions.hhi_calculator(['SUB_C','SBN_C','BD_C', 'CL_C', 'SHORT_C', 'OTHERA_C'], 'TOTALDEBT_C', 'HH2_C', FUNDADEBT)
#FUNDADEBT = Functions.hhi_calculator(['SUB_C','SBN_C','BDB_C', 'CL_C', 'OTHERA_C'], 'TOTALDEBT_C', 'HH3_C', FUNDADEBT)
#FUNDADEBT = Functions.hhi_calculator(['SUBNOTCONV_C','SUBCONV_C','CONV_C', 'DD_C', 'DN_C', 'BD_C', 'CL_C','SHORT_C',
                                      #'OTHERA2_C'],'TOTALDEBT_C', 'HH4_C', FUNDADEBT)


FUNDADEBT = Functions.hhi_calculator(['SUB_C','SBN_C','BD_C', 'CL_C', 'SHORT_C'], 'TOTALDEBT_C', 'HH1', FUNDADEBT)
FUNDADEBT = Functions.hhi_calculator(['SUBNOTCONV_C','SUBCONV_C','CONV_C', 'DD_C', 'DN_C', 'BD_C', 'CL_C','SHORT_C',],
                                     'TOTALDEBT_C', 'HH2', FUNDADEBT)

FUNDADEBT['TOTALDEBT_C_2'] = FUNDADEBT['SUBNOTCONV_C'] + FUNDADEBT['SUBCONV_C'] +\
                         FUNDADEBT['CONV_C'] + FUNDADEBT['DD_C'] + FUNDADEBT['DN_C'] + FUNDADEBT['BD_C'] +\
                         FUNDADEBT['CL_C'] + FUNDADEBT['SHORT_C']+  FUNDADEBT['cmp']


FUNDADEBT = Functions.hhi_calculator(['SUB_C','SBN_C','BD_C', 'CL_C', 'SHORT_C'], 'TOTALDEBT_C_2', 'HH1B', FUNDADEBT)
FUNDADEBT = Functions.hhi_calculator(['SUBNOTCONV_C','SUBCONV_C','CONV_C', 'DD_C', 'DN_C', 'BD_C', 'CL_C','SHORT_C',],
                                     'TOTALDEBT_C_2', 'HH2B', FUNDADEBT)

#New percentages

list_sum2 = ['SUB_C','SBN_C','BD_C', 'CL_C', 'SHORT_C', 'cmp', 'OTHERA_C']
FUNDADEBT = Functions.pct_calculator(list_sum2, 'TOTALDEBT_C', 'PCT', FUNDADEBT)
list_sum2 = ['SUBNOTCONV_C','SUBCONV_C','CONV_C','DD_C', 'DN_C', 'BD_C', 'CL_C', 'SHORT_C', 'cmp', 'OTHERA2_C']
FUNDADEBT = Functions.pct_calculator(list_sum2, 'TOTALDEBT_C', 'PCT', FUNDADEBT)




list_funda = ['TOTALDEBT_C', 'SUB_C', 'SBN_C', 'BD_C', 'CL_C', 'SHORT_C', 'SUBPCT_C',
              'SUBPCT_C', 'CLPCT_C', 'BDPCT_C', 'SBNPCT_C', 'SHORTPCT_C', 'CURLIAPCT_C',
              'CMPPCT_C', 'HH1_C', 'HH2_C', 'HH3_C']

to_drop = ['CHECK_4', 'CHECK_C', 'CHECK_CO', 'CHECK_2', 'CHECK_3', 'sum_temp', 'SUB_CTEMP', 'SBN_CTEMP', 'BD_CTEMP',
           'CL_CTEMP', 'cmpTEMP', 'SUBNOTCONV_CTEMP', 'CONV_CTEMP', 'DD_CTEMP', 'DN_CTEMP']

FUNDADEBT = FUNDADEBT.drop(to_drop, axis=1)

to_drop = ['CHECK_4', 'CHECK_C', 'CHECK_CO', 'CHECK_2', 'CHECK_3', 'sum_temp', 'SUB_CTEMP', 'SBN_CTEMP', 'BD_CTEMP',
           'CL_CTEMP', 'cmpTEMP', 'SUBNOTCONV_CTEMP', 'CONV_CTEMP', 'DD_CTEMP', 'DN_CTEMP']

FUNDADEBT.columns.str.endswith('_fn')
FUNDADEBT = FUNDADEBT.loc[:, ~FUNDADEBT.columns.str.endswith('_fn')]
FUNDADEBT = FUNDADEBT.loc[:,~FUNDADEBT.columns.str.endswith('_dc')]

FUNDADEBT.to_csv(os.path.join(datadirectory, "fundadebtprocessedNOV.csv"))
FUNDADEBT = pd.read_csv(os.path.join(datadirectory, "collapsedlink.csv"), index_col=0)
#Separate HHI
# Reliance on one debt type, how to do this
#>10%

FUNDADEBTC = pd.read_csv(os.path.join(datadirectory, "FUNDADEBT19502018.gz"), sep='\t')
FUNDADEBTC['datadate'] = pd.to_datetime(FUNDADEBTC['datadate'], format='%Y%m%d')
FUNDADEBTC = FUNDADEBTC[FUNDADEBTC.indfmt == 'INDL']
FUNDADEBT_SMALL = FUNDADEBTC[FUNDADEBTC.fyear>=1969]
FUNDADEBT = FUNDADEBT.drop(to_drop, axis=1)

FUNDADEBT_SMALL['D1'] = FUNDADEBT_SMALL['dd1']
FUNDADEBT['CHECK_DEBT'] = FUNDADEBT['dd'] + FUNDADEBT['dn'] + FUNDADEBT['dcvt'] + \
                          FUNDADEBT['dlto'] + FUNDADEBT['ds'] + FUNDADEBT['dclo']

FUNDADEBT['CCC'] = FUNDADEBT['dltt'] - FUNDADEBT['CHECK_DEBT']
1.433
0.828
0.538
0.475

FUNDADEBT['dcvsub'] + FUNDADEBT['ds']
FUNDADEBT['SBN_C'] = FUNDADEBT['dd'] + FUNDADEBT['dn'] + FUNDADEBT['dcvt'] - FUNDADEBT['dcvsub']
FUNDADEBT['BD_C'] = FUNDADEBT['dlto'] - FUNDADEBT['cmp']
FUNDADEBT['CL_C'] = FUNDADEBT['dclo']
del FUNDADEBT
del FUNDACMP
#OLDERR

FUNDADEBT.to_csv(os.path.join(datadirectory, "fundadebtprocessed.csv"))