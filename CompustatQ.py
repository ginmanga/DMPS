#Calculate quarterly stadard deviations, by company and by SIC codes
#get SIC codes from CRSP monthly file


#load quarter data
#calculate quarterly (12) stdeviation of cash flow and sales per firm
# do it at industyr level use SIC codes Fama french 48 industy


FUNDAQ = pd.read_csv(os.path.join(datadirectory,'processed', "FUNDAQDEC4.gz"), index_col=0)
FUNDAQ = FUNDAQ.dropna(subset=['datacqtr'])

FUNDAQ['datadate'] = pd.to_datetime(FUNDAQ['datadate'])
FUNDAQAT = pd.read_csv(os.path.join(datadirectory, "FUNDAQAT2018.gz"), sep='\t')
FUNDAQAT['datadate'] = pd.to_datetime(FUNDAQAT['datadate'], format='%Y%m%d')
FUNDAQAT['gvkey'] = FUNDAQAT['gvkey'].apply(str)
FUNDAQ['gvkey'] = FUNDAQ['gvkey'].apply(str)



FUNDAQAT.sort_values(['gvkey','datadate','fqtr'], inplace=True)
FUNDAQAT.drop_duplicates(subset=['gvkey','datadate','atq'], keep='first', inplace=True)

FUNDAQAT.sort_values(['gvkey','datadate','atq'], inplace=True)
FUNDAQAT.drop_duplicates(subset=['gvkey','datadate'], keep='first', inplace=True)

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
#FUNDAQ.shape

FUNDAQ['NEWSALE'] = FUNDAQ['saleq']
FUNDAQ['NEWSALE'] = FUNDAQ['NEWSALE'].fillna(FUNDAQ['sale'])
FUNDAQ['NEWSALE'] = np.where(FUNDAQ['NEWSALE'] > FUNDAQ['sale'], FUNDAQ['sale'], FUNDAQ['NEWSALE'])
#FUNDAQ.shape
#interpolate total assets
FUNDAQ['NEWATT'] = FUNDAQ['NEWAT']
#FUNDAQ[FUNDAQ.gvkey=='1076'][50:70]
FUNDAQQ = FUNDAQ[['gvkey','datadate','NEWATT']]
FUNDAQQ = FUNDAQQ.groupby('gvkey').apply(lambda group: group.interpolate(method='index'))
#FUNDAQQ.shape
#FUNDAQQ[FUNDAQQ.gvkey=='1076'][20:70]
FUNDAQ = pd.merge(FUNDAQ,  FUNDAQQ[['gvkey','datadate','NEWATT']],
                         left_on=['gvkey','datadate'],
                         right_on = ['gvkey','datadate'], how='left')
#FUNDAQ[FUNDAQ.gvkey=='1076'][50:70]
#FUNDAQ.shape
#create sale/at variable
FUNDAQ['saleat'] = FUNDAQ['NEWSALE']/FUNDAQ['NEWATT_y']
FUNDAQ['oiadpqat'] = FUNDAQ['oiadpq']/FUNDAQ['NEWATT_y']
#cleanup
#del FUNDAQQ
#del FUNDAQAT
#del FUNDABSSMALL
#del bb
#FUNDAQ = FUNDAQ.drop('sale_std_ff48', axis=1)

#FUNDAQ = FUNDAQ.drop(['temp1','temp2','new_var','saleat_cut'], axis=1)


#Match FUNDAAQ to FUNDAABS nearest total assets to quarter

#calculate quarterly std using sales and FF48, calculate a 12 qtr rolling and 9qtr rolling, by FF48 and by firm
#calculate annual std using sales and FF48, calculate a 4 year rolling by FF48 and by firm
#remove financials and utilities before calculating, create functions to automate the process
#FUNDAQ.shape
FUNDAQ = Functions.winsor(FUNDAQ, column = ['saleat'])
FUNDAQ = Functions.winsor(FUNDAQ, column = ['oiadpqat'])

#build function to calculate the standard devs in different situations

FUNDAQ, c = Functions.rol_vars(FUNDAQ, 'saleat_cut', 'sale_std_ff48_12', group=['gvkey', 'datacqtr'], onn='datacqtr',
                    window = 12, levels=2, group1 = ['gvkey','datacqtr','FF48'], group2=['FF48', 'datacqtr'])

FUNDAQ = Functions.rol_vars(FUNDAQ, 'saleat_cut', 'sale_std_ff48_9', group=['gvkey', 'datacqtr'], onn='datacqtr',
                    window = 9, levels=2, group1 = ['gvkey','datacqtr','FF48'], group2=['FF48', 'datacqtr'])

FUNDAQ.shape

FUNDAQ = Functions.rol_vars(FUNDAQ, 'saleat_cut', 'sale_std_12', group=['gvkey', 'datacqtr'], onn='datacqtr',
                    window = 12, levels=1)


FUNDAQ = Functions.rol_vars(FUNDAQ, 'saleat_cut', 'sale_std_9', group=['gvkey', 'datacqtr'], onn='datacqtr',
                    window = 9, levels=1)

#income
FUNDAQ = Functions.rol_vars(FUNDAQ, 'oiadpq_cut', 'income_std_12', group=['gvkey', 'datacqtr'], onn='datacqtr',
                    window = 12, levels=1)


FUNDAQ = Functions.rol_vars(FUNDAQ, 'oiadpq_cut', 'income_std_9', group=['gvkey', 'datacqtr'], onn='datacqtr',
                    window = 9, levels=1)


#SAVE
FUNDAQ.to_csv(os.path.join(datadirectory, "FUNDAQDEC8STD.csv.gz"), index=False, compression='gzip')
c.to_csv(os.path.join(datadirectory, "FF48SALE12.gz"), index=False, compression='gzip')

