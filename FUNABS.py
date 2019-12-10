#FUNDABS main database

FUNDAMAINBS7018
AT Total assets
Sale
PRCC_F
CSHFD common shares used to cal earning per share
CSHO common shares outstanding
PSTKL preferred stock liquidating value
TXDITC deferred taxes and investment tax credit
OIBDP operating income before depreciation
DVC dividends common
DVT ddividends total
CHE cash and short-term investments
PPENT proerty etc
CAPX capital expendiotrues
XSGA selling general and admin expenses
XRD research and development
CEQ common/ordinary equity  total
APALCH accounts payable and AL increase decrewase
RECCH accounts receivable decrease
WCAP working capital
DLTIS long-term debt issuance
DLTR long-term debt reduction
DLCCH current debt changes
SSTK sale of comon and preferred stock
PRSTKC purchase of common and preferred stock

#merge with fundadebt for total debt and with COMPUCRSPIQCR for LINKDT to calculate age
#from debt get TOTALDEBT SUB SBN BD CL SHORT CHECK HH1 HH2


#FUNDABS = pd.read_csv(os.path.join(datadirectory, "fundamainBS7018.gz"), sep='\t')
#FUNDABS['datadate'] = pd.to_datetime(FUNDABS['datadate'], format='%Y%m%d')


FUNDADEBT = pd.read_csv(os.path.join(datadirectory, 'processed', "fundadebtprocessedNOV.gz"), index_col=0)
FUNDADEBT['datadate'] = pd.to_datetime(FUNDADEBT['datadate'])


c = ['gvkey','datadate', 'TOTALDEBT_C']

#FUNDADEBT = FUNDADEBT.loc[:, ~FUNDADEBT.columns.str.endswith('_fn')]
#FUNDADEBT = FUNDADEBT.loc[:,~FUNDADEBT.columns.str.endswith('_dc')]


FUNDABS = pd.merge(FUNDABS,
                   FUNDADEBT[c],
                   left_on=['gvkey','datadate'],
                   right_on = ['gvkey','datadate'],
                   how='left')

#get sic_ch and FF48
FUNDASIC = pd.read_csv(os.path.join(datadirectory, 'processed', "FUNDASIC_DEC4.gz"), index_col=0)
FUNDASIC['datadate'] = pd.to_datetime(FUNDASIC['datadate'])

FUNDABS = pd.merge(FUNDABS,
                   FUNDASIC[['gvkey','datadate','sic_ch', 'FF48']],
                   left_on=['gvkey','datadate'],
                   right_on = ['gvkey','datadate'],
                   how='left')

#Erase financials and utilities
f2 = list(range(4900, 4949)) #delete utilities
f3 = list(range(6000, 6999)) #delete utilities
FUNDABS['util'] = [1 if x in f2 else 0 for x in FUNDABS['sic_ch']]
FUNDABS['fin'] = [1 if x in f3 else 0 for x in FUNDABS['sic_ch']]
FUNDABS = FUNDABS[FUNDABS.util == 0]
FUNDABS = FUNDABS[FUNDABS.fin == 0]
FUNDABS = FUNDABS.drop(columns=['util','fin'])
# Calculate STD of sales and earninings
FUNDABS['PROF'] = FUNDABS['oibdp']/FUNDABS['at']
FUNDABS['SALE'] = FUNDABS['sale']/FUNDABS['at']
list_variables_WINSOR = ['PROF','SALE']
FUNDABS = Functions.winsor(FUNDABS, column = list_variables_WINSOR)

FUNDABS, c = Functions.rol_vars(FUNDABS, 'SALE_cut', 'sale_std_ff48_4', group=['gvkey', 'datadate'], onn='datadate',
                    window = 4, levels=2, group1 = ['gvkey','datadate','fyear','FF48'], group2=['FF48', 'fyear'])

#FUNDABS = Functions.rol_vars(FUNDABS, 'SALE_cut', 'sale_std_ff48_4_2', group=['gvkey', 'fyear'], onn='fyear',
                   # window = 4, levels=2, group1 = ['gvkey','fyear','FF48'], group2=['FF48', 'fyear'])

#FUNDABS = FUNDABS.drop(columns=['sale_std_ff48_4_2 '])

c_bs= ['gvkey','datadate', 'ap', 'at','sale','prcc_f', 'cshfd', 'pstkl', 'txditc', 'oibdp',
       'dvc','dvt','che','ppent','capx','xsga','xrd','ceq', 'TOTALDEBT_C', 'HHI','HHI2',
       'SUBPERCENT', 'CLPERCENT','BDPERCENT','SBNPERCENT','SHORTPERCENT', 'CURLIAPERCENT',
       'CHECK']

c_bs= ['gvkey','datadate', 'fyear', 'ap', 'at','sale','prcc_f', 'cshfd', 'pstkl', 'txditc', 'oibdp',
       'dvc','dvt','che','ppent','capx','xsga','xrd','ceq', 'TOTALDEBT_C']
#Replace missing with 0 or remove
list_replace = ['ap','cshfd', 'pstkl', 'txditc', 'oibdp',
                'dvc','dvt','che','ppent','capx','xsga','xrd','ceq']

list_remove = ['at', 'TOTALDEBT_C', 'prcc_f']

BS1DF = FUNDABS[c_bs]

for i in list_replace:
    BS1DF[i].fillna(0, inplace=True)

for i in list_remove:
    BS1DF = BS1DF.dropna(subset=[i])

#build balance sheet variables
BS1DF['MVEquity'] = BS1DF['prcc_f']*BS1DF['cshfd']
BS1DF['MVBook'] = (BS1DF['MVEquity']+BS1DF['TOTALDEBT_C'] -
                   BS1DF['pstkl'] - BS1DF['txditc'])/BS1DF['at']
BS1DF['DIVP'] = np.where(BS1DF['dvc'] > 0, 1, 0) #dividend payer
BS1DF['MLEV'] = BS1DF['TOTALDEBT_C']/(BS1DF['TOTALDEBT_C'] + BS1DF['MVEquity']) #tangf


BS1DF['PROF'] = BS1DF['oibdp']/BS1DF['at'] #profitability
BS1DF['CASH'] = BS1DF['che']/BS1DF['at']
BS1DF['TANG'] = BS1DF['ppent']/BS1DF['at'] #tangf
BS1DF['CAPEX'] =  BS1DF['capx']/BS1DF['at']
BS1DF['ADVERT'] = BS1DF['xsga']/BS1DF['at']
BS1DF['RD'] = BS1DF['xrd']/BS1DF['at']
BS1DF['AP'] = BS1DF['ap']/BS1DF['at']
BS1DF['BLEV'] = BS1DF['TOTALDEBT_C']/BS1DF['at']

ratios = ['oibdp','che','ppent','capx','xsga','xrd','ap','TOTALDEBT_C']
names = ['PROF','CASH','TANG','CAPEX','ADVERT','RD','AP','BLEV']
BS1DF = Functions.fin_ratio(BS1DF, ratios, names=[], 'at')

list_variables = ['MVEquity','MVBook', 'PROF', 'DIVP','CASH', 'TANG', 'CAPEX', 'ADVERT', 'RD', 'MLEV', 'BLEV']




#for later: product uniqueness, CF volatility, asset maturity

#Winsorize, add sic_ch and FF48, calculate standard deviation of sales and earnings to at usuing annual data
list_variables_WINSOR = ['MVBook', 'PROF','CASH', 'TANG', 'CAPEX', 'ADVERT', 'RD', 'MLEV', 'BLEV']
BS1DF = Functions.winsor(BS1DF, column = list_variables_WINSOR)

#Add and remove variables only the needed ones
#delete original ratios and rename the cuts
list_variables_WINSOR = ['MVBook', 'PROF','CASH', 'TANG', 'CAPEX', 'ADVERT', 'RD', 'MLEV', 'BLEV']
BS1DF = BS1DF.drop(list_variables_WINSOR, axis=1)
list_variables_re = [i + '_cut' for i in list_variables_WINSOR]
for i in list_variables_re:
    BS1DF.rename(columns={i: i.replace('_cut','')}, inplace=True)



#Add volatility variables
#First add quartelry ones to FUNDABS, then add them to BS1DF
#FUNDAQ.to_csv(os.path.join(datadirectory, "FUNDAQDEC8STD.csv.gz"), index=False, compression='gzip')
FUNDAQ = pd.read_csv(os.path.join(datadirectory,'processed', "FUNDAQDEC8STD.gz"))
FUNDAQ['datadate'] = pd.to_datetime(FUNDAQ['datadate'])

to_keep = ['gvkey','datadate','sale_std_12','sale_std_9','sale_std_ff48_12','sale_std_ff48_9',
           'income_std_12','income_std_9']
FUNDABS = pd.merge(FUNDABS,
                   FUNDAQ[to_keep],
                    left_on=['gvkey','datadate'],
                    right_on = ['gvkey','datadate'], how='left')



#Calculate age using all compustat
a = FUNDABS.groupby(['gvkey']).cumcount().reset_index(name='counts')
a['counts'] = a['counts'] + 1
#FUNDABS.groupby(['gvkey']).cumcount().reset_index(name='counts')
ce = FUNDABS.join(a)
ce.rename(columns={'counts':'AGE'}, inplace=True)

to_keep = ['gvkey','datadate','sale_std_12','sale_std_9','sale_std_ff48_12','sale_std_ff48_9',
           'income_std_12','income_std_9','FF48','sic_ch']
BS1DF = pd.merge(BS1DF,
                FUNDABS[to_keep],
                left_on=['gvkey','datadate'],
                right_on = ['gvkey','datadate'], how='left')


BS1DF = pd.merge(BS1DF,
                ce[['gvkey','datadate','AGE']],
                left_on=['gvkey','datadate'],
                right_on = ['gvkey','datadate'], how='left')

BS1DF.to_csv(os.path.join(datadirectory, "BS1DF-temp.csv"), index=False)

BS1DF = pd.read_csv(os.path.join(datadirectory, "BS1DF-temp.csv"))
BS1DF['datadate'] = pd.to_datetime(BS1DF['datadate'])
#Translate quarter into date and match back to BS1DF
c = pd.read_csv(os.path.join(datadirectory, "FF48SALE12.gz"))
c['datadate'] = c['datacqtr'].str.replace(r'(\d+)(Q\d)', r'\1-\2')
#c['datadate'] = pd.PeriodIndex(c['datadate'], freq='Q').to_timestamp()
c['datadate'] = pd.to_datetime(c['datadate'])

#MATCH USING CLOSEST

#BS1DF = BS1DF.drop(columns=['temp_x','tempdays_x', 'temp_y','tempdays_y'])
#BS1DF.rename(columns={'datadate_x': 'datadate'}, inplace=True)
#BS1DF.rename(columns={'FF48': 'FF48'}, inplace=True)
#BS1DF_small = BS1DF[BS1DF.fyear < 1980]
cc = c[['FF48','datadate','sale_std_ff48_12']]
BS1DF =  Functions.match_closest(BS1DF, cc, 'FF48', 'datadate', direction='nearest')
BS1DF = BS1DF.sort_values(by=['gvkey','datadate'])

#BS1DF.to_csv(os.path.join(datadirectory, "BSprocessedNOV27.csv"), index=False)
def match_closest(data1, data2, key1, date1, key2=0, date2=0, direction='backward'):
#Add crsp EXCH and nation
FUNDALIST_CRSPIDS = pd.read_csv(os.path.join(datadirectory, 'processed', "FUNDALIST_CRSPIDSDEC4.gz"), index_col=0)
FUNDADEBT['datadate'] = pd.to_datetime(FUNDADEBT['datadate'])