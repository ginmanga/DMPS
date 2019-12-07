#Calculate quarterly stadard deviations, by company and by SIC codes
#get SIC codes from CRSP monthly file


#load quarter data
#calculate quarterly (12) stdeviation of cash flow and sales per firm
# do it at industyr level use SIC codes Fama french 48 industy


FUNDAQ = pd.read_csv(os.path.join(datadirectory,'processed', "FUNDAQDEC4.csv"), index_col=0)
FUNDAQ['datadate'] = pd.to_datetime(FUNDAQ['datadate'])
FUNDAQAT = pd.read_csv(os.path.join(datadirectory, "FUNDAQAT2018.gz"), sep='\t')
FUNDAQAT['datadate'] = pd.to_datetime(FUNDAQAT['datadate'], format='%Y%m%d')
FUNDAQAT['gvkey'] = FUNDAQAT['gvkey'].apply(str)
FUNDAQ['gvkey'] = FUNDAQ['gvkey'].apply(str)
#FUNDADEBT = FUNDADEBT[FUNDADEBT.indfmt == 'INDL']
#calculate sale and cash flow volatility using quarterly data and by FF48
FUNDABSSMALL = FUNDABS[['gvkey','datadate','at', 'sale']]
FUNDABSSMALL['gvkey'] = FUNDABSSMALL['gvkey'].apply(str)

FUNDAQ = pd.merge(FUNDAQ,  FUNDAQAT[['gvkey','datadate','atq']],
                         left_on=['gvkey','datadate'],
                         right_on = ['gvkey','datadate'], how='left')

FUNDAQ = pd.merge(FUNDAQ,  FUNDABSSMALL[['gvkey','datadate','at', 'sale']],
                         left_on=['gvkey','datadate'],
                         right_on = ['gvkey','datadate'], how='left')

#create new sales at variable combining quarterly and annual
FUNDAQ['NEWAT'] = FUNDAQ['atq']
FUNDAQ['NEWAT'] = FUNDAQ['NEWAT'].fillna(FUNDAQ['at'])

FUNDAQ['NEWSALE'] = FUNDAQ['saleq']
FUNDAQ['NEWSALE'] = FUNDAQ['NEWSALE'].fillna(FUNDAQ['sale'])
FUNDAQ['NEWSALE'] = np.where(FUNDAQ['NEWSALE'] > FUNDAQ['sale'], FUNDAQ['sale'], FUNDAQ['NEWSALE'])

#interpolate total assets
FUNDAQ['NEWATT'] = FUNDAQ['NEWAT']
FUNDAQQ = FUNDAQ[['gvkey','datadate','NEWATT']]
FUNDAQQ = FUNDAQQ.groupby('gvkey').apply(lambda group: group.interpolate(method='index'))


FUNDAQ = pd.merge(FUNDAQ,  FUNDAQQ[['gvkey','datadate','NEWATT']],
                         left_on=['gvkey','datadate'],
                         right_on = ['gvkey','datadate'], how='left')


#create sale/at variable
FUNDAQ['saleat'] = FUNDAQ['NEWSALE']/FUNDAQ['NEWATT_y']
#cleanup
del FUNDAQQ
del FUNDAQAT
del FUNDABSSMALL
del bb
FUNDAQ = FUNDAQ.drop('sale_std_ff48', axis=1)

FUNDAQ = FUNDAQ.drop(['temp1','temp2','new_var','saleat_cut'], axis=1)

cc = pd.merge(FUNDAQ,  FUNDAQQ[['gvkey','datadate','atq']],
                         left_on=['gvkey','datadate'],
                         right_on = ['gvkey','datadate'], how='left')

#Match FUNDAAQ to FUNDAABS nearest total assets to quarter

#calculate quarterly std using sales and FF48, calculate a 12 qtr rolling and 9qtr rolling, by FF48 and by firm
#calculate annual std using sales and FF48, calculate a 4 year rolling by FF48 and by firm
#remove financials and utilities before calculating, create functions to automate the process
FUNDAQ['saleat_99'] = FUNDAQ['saleat'].quantile(.99)
FUNDAQ['saleat_01'] = FUNDAQ['saleat'].quantile(.01)
FUNDAQ['saleat_cut'] = np.where(FUNDAQ['saleat'] < FUNDAQ['saleat_99'], FUNDAQ['saleat'], FUNDAQ['saleat_99'])
FUNDAQ['saleat_cut'] = np.where(FUNDAQ['saleat_cut'] < FUNDAQ['saleat_01'], FUNDAQ['saleat_01'], FUNDAQ['saleat_cut'])
FUNDAQ['saleat_cut'] = np.where(FUNDAQ['saleat'].isna, FUNDAQ['saleat'], FUNDAQ['saleat_cut'])
FUNDAQ = FUNDAQ.drop('saleat_99', axis=1)
FUNDAQ = FUNDAQ.drop('saleat_01', axis=1)


FUNDAQNEW = Functions.winsor(FUNDAQ, column = ['saleat'])

#build function to calculate the standard devs in different situations




FUNDAQ[['gvkey','datacqtr','FF48', 'saleat_cut']]
FUNDAQ[['gvkey','datacqtr', 'saleat_cut']]

FUNDAQ = Functions.rol_vars(FUNDAQ, 'saleat_cut', 'sale_std_ff48_12', group=['gvkey', 'datacqtr'],
                    group1 = ['gvkey','datacqtr','FF48'], group2=['FF48', 'datacqtr'], onn='datacqtr',
                    window = 12, levels=2)

FUNDAQ = Functions.rol_vars(FUNDAQ, 'saleat_cut', 'sale_std_ff48_9', group=['gvkey', 'datacqtr'],
                    group1 = ['gvkey','datacqtr','FF48'], group2=['FF48', 'datacqtr'], onn='datacqtr',
                    window = 9, levels=2)

FUNDAQ = Functions.rol_vars(FUNDAQ, 'saleat_cut', 'sale_std_12', group=['gvkey', 'datacqtr'], onn='datacqtr',
                    window = 12, levels=1)

FUNDAQ = Functions.rol_vars(FUNDAQ, 'saleat_cut', 'sale_std_9', group=['gvkey', 'datacqtr'], onn='datacqtr',
                    window = 9, levels=1)

o = getattr(series_temp, i)
get_meth = getattr(series_temp[o == 1].groupby(['fyear'])[[a[0]]], method)
series_temp_1 = get_meth().reset_index()

c = FUNDAQ.groupby('gvkey').rolling(12, on='datacqtr').saleat.std().reset_index()
c = pd.merge(c,  FUNDAQ[['gvkey','datacqtr','FF48']],
                         left_on=['gvkey','datacqtr'],
                         right_on = ['gvkey','datacqtr'], how='left')
c = c.dropna(subset=['saleat'])


c = c.groupby(['FF48', 'datacqtr']).saleat.mean().reset_index()
c.rename(columns={'saleat': 'sale_std_ff48_9'}, inplace=True)

cc = c.groupby(['FF48', 'datacqtr']).sale_std_ff48_9.max().reset_index()

FUNDAQ = pd.merge(FUNDAQ, c,
                         left_on=['FF48','datacqtr'],
                         right_on = ['FF48','datacqtr'], how='left')




#b = FUNDAQ.groupby('gvkey').rolling(12).saleq.mean().reset_index()
b = FUNDAQ.groupby('gvkey').rolling(12, on='datacqtr').saleq.mean().reset_index()
bb = FUNDAQQ.groupby('gvkey').rolling(12, on='datacqtr').saleq.mean().reset_index()
c = FUNDAQ.groupby('gvkey').rolling(12, on='datacqtr').saleq.std().reset_index()

d = FUNDAQ.groupby(['FF48','datacqtr']).rolling(12).saleq.std().reset_index()

cc = pd.merge(c,  FUNDAQ[['gvkey','datacqtr','FF48']],
                         left_on=['gvkey','datacqtr'],
                         right_on = ['gvkey','datacqtr'], how='left')
cc = cc.dropna(subset=['saleq'])
ccc = cc.groupby(['FF48', 'datacqtr']).saleq.mean().reset_index()
cccc = cc.groupby(['FF48', 'datacqtr']).saleq.count().reset_index()

ccc = pd.merge(ccc,  cccc,
                         left_on=['FF48','datacqtr'],
                         right_on = ['FF48','datacqtr'], how='left')

