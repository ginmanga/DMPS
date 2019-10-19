#First, match COMPUSTAT, CRSP and CAPITALIQ make sure to add CUSIP and CIK to the table to merge with other datasets
#Calculate debt measures
#Compustat:
# SUB = dcvsub + ds
# SBN = DD+DN
# BD = DLTO - CMP
# CL = CL
# CMP = CMP
#short = dlc-dd1
import Functions
datadirectory = os.path.join(os.getcwd(), 'data')

FUNDADEBT = pd.merge(FUNDADEBT,
                    FUNDACMP[['gvkey','datadate','cmp']],
                    left_on=['gvkey','datadate'],
                    right_on = ['gvkey','datadate'], how='left')

list_replace =['dltt','cmp','dcvsub','ds','dd','dn','dlto','dlc','dd1','dclo','dcvt','dltp']
for i in list_replace:
    FUNDADEBT[i].fillna(0, inplace=True)

FUNDADEBT['TOTALDEBT_C'] = FUNDADEBT['dltt'] + FUNDADEBT['dlc']
FUNDADEBT['SUB_C'] = FUNDADEBT['dcvsub'] + FUNDADEBT['ds']
FUNDADEBT['SBN_C'] = FUNDADEBT['dd'] + FUNDADEBT['dn'] + FUNDADEBT['dcvt'] - FUNDADEBT['dcvsub']
FUNDADEBT['BD_C'] = FUNDADEBT['dlto'] - FUNDADEBT['cmp']
FUNDADEBT['CL_C'] = FUNDADEBT['dclo']
FUNDADEBT['SHORT_C'] = FUNDADEBT['dlc'] - FUNDADEBT['dd1']
FUNDADEBT['OTHER_C'] = FUNDADEBT['TOTALDEBT_C'] - FUNDADEBT['SUB_C'] - FUNDADEBT['SBN_C'] - FUNDADEBT['BD_C'] \
                       - FUNDADEBT['CL_C'] - FUNDADEBT['SHORT_C'] - FUNDADEBT['cmp']

FUNDADEBT['BDB_C'] = FUNDADEBT['BD_C'] + FUNDADEBT['SHORT_C']


### correct for long-term debt
list_sum1 = ['SUB_C','SBN_C','BD_C', 'CL_C', 'SHORT_C', 'cmp']
list_sum2 = ['SUB_C','SBN_C','BD_C', 'CL_C', 'SHORT_C', 'cmp', 'OTHER_C']
FUNDADEBT['CHECK_C'] = (FUNDADEBT[list_sum2].sum(axis=1))/FUNDADEBT['TOTALDEBT_C']
FUNDADEBT['CHECK_CO'] = FUNDADEBT[list_sum1].sum(axis=1)/FUNDADEBT['TOTALDEBT_C']
FUNDADEBT['CHECK_2'] = FUNDADEBT[list_sum1].sum(axis=1) - FUNDADEBT['TOTALDEBT_C'] + FUNDADEBT['dd1']
FUNDADEBT['CHECK_3'] = np.where(FUNDADEBT['CHECK_2'].round(1) == 0,1,0) \
                        * np.where(FUNDADEBT['CHECK_CO'] == 1, 0, 1)

# Adjustments, check sTotal_debt - sum total
FUNDADEBT['sum_temp'] = FUNDADEBT[['SUB_C','SBN_C','BD_C', 'CL_C', 'cmp']].sum(axis=1)
FUNDADEBT = Functions.pct_calculator(['SUB_C','SBN_C','BD_C', 'CL_C', 'cmp'],
                                     'sum_temp', 'TEMP', FUNDADEBT)



#FUNDADEBT['CHECK_2_c'] = FUNDADEBT['CHECK_2'].round(1)
#FUNDADEBT['CHECK_COc'] = FUNDADEBT['CHECK_3'].round(1)

#FUNDADEBT['CHECK_33'] = np.where(FUNDADEBT['CHECK_2'].round(1) == 0,1,0) \
                        #* np.where(FUNDADEBT['CHECK_CO'].round(2)  == 1, 0, 1)

FUNDADEBT['SUB_C'] = FUNDADEBT['SUB_C'] + FUNDADEBT['dd1']*FUNDADEBT['SUB_CTEMP']*FUNDADEBT['CHECK_3']
FUNDADEBT['SBN_C'] = FUNDADEBT['SBN_C'] + FUNDADEBT['dd1']*FUNDADEBT['SBN_CTEMP']*FUNDADEBT['CHECK_3']
FUNDADEBT['BD_C'] = FUNDADEBT['BD_C'] + FUNDADEBT['dd1']*FUNDADEBT['BD_CTEMP']*FUNDADEBT['CHECK_3']
FUNDADEBT['CL_C'] = FUNDADEBT['CL_C'] + FUNDADEBT['dd1']*FUNDADEBT['CL_CTEMP']*FUNDADEBT['CHECK_3']
FUNDADEBT['cmp'] = FUNDADEBT['cmp'] + FUNDADEBT['dd1']*FUNDADEBT['cmpTEMP']*FUNDADEBT['CHECK_3']

FUNDADEBT['CHECK_4'] = (FUNDADEBT[list_sum1].sum(axis=1))/FUNDADEBT['TOTALDEBT_C']






FUNDADEBT['CHECK_CO2'] = (FUNDADEBT['SHORT_C']+FUNDADEBT['SUB_C'] + FUNDADEBT['SBN_C'] +
                        FUNDADEBT['BD_C']+ FUNDADEBT['CL_C'] +
                        FUNDADEBT['cmp'])/FUNDADEBT['TOTALDEBT_C']






FUNDADEBT = Functions.hhi_calculator(['SUB_C','SBN_C','BD_C', 'CL_C', 'SHORT_C', 'cmp','OTHER_C'], 'TOTALDEBT_C', 'HH1_C', FUNDADEBT)
FUNDADEBT = Functions.hhi_calculator(['SUB_C','SBN_C','BD_C', 'CL_C', 'SHORT_C', 'OTHER_C'], 'TOTALDEBT_C', 'HH2_C_t', FUNDADEBT)
FUNDADEBT = Functions.hhi_calculator(['SUB_C','SBN_C','BDB_C', 'CL_C', 'OTHER_C'], 'TOTALDEBT_C', 'HH3_C_t', FUNDADEBT)



list_funda = ['TOTALDEBT_C', 'SUB_C', 'SBN_C', 'BD_C', 'CL_C', 'SHORT_C', 'SUBPCT_C',
              'SUBPCT_C', 'CLPCT_C', 'BDPCT_C', 'SBNPCT_C', 'SHORTPCT_C', 'CURLIAPCT_C',
              'CMPPCT_C', 'HH1_C', 'HH2_C', 'HH3_C']

datadirectory
FUNDADEBT.to_csv(os.path.join(datadirectory_2, "fundadebtprocessed.csv"))
