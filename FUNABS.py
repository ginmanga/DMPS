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


FUNDABS = pd.read_csv(os.path.join(datadirectory, "fundamainBS7018.gz"), sep='\t')
FUNDABS['datadate'] = pd.to_datetime(FUNDABS['datadate'], format='%Y%m%d')
FUNDADEBT = pd.read_csv(os.path.join(datadirectory, "fundadebtprocessed.csv"), index_col=0)
FUNDADEBT['datadate'] = pd.to_datetime(FUNDADEBT['datadate'])

c = ['gvkey','datadate', 'TOTALDEBT_C']

FUNDADEBT.dtypes

FUNDABS = pd.merge(FUNDABS,
                   FUNDADEBT[c],
                   left_on=['gvkey','datadate'],
                   right_on = ['gvkey','datadate'],
                   how='left')

c_bs= ['gvkey','datadate', 'at','sale','prcc_f', 'cshfd', 'pstkl', 'txditc', 'oibdp',
       'dvc','dvt','che','ppent','capx','xsga','xrd','ceq', 'TOTALDEBT_C', 'HHI','HHI2',
       'SUBPERCENT', 'CLPERCENT','BDPERCENT','SBNPERCENT','SHORTPERCENT', 'CURLIAPERCENT',
       'CHECK']

c_bs= ['gvkey','datadate', 'at','sale','prcc_f', 'cshfd', 'pstkl', 'txditc', 'oibdp',
       'dvc','dvt','che','ppent','capx','xsga','xrd','ceq', 'TOTALDEBT_C']
#Replace missing with 0 or remove
list_replace = ['cshfd', 'pstkl', 'txditc', 'oibdp',
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
BS1DF['PROF'] = BS1DF['oibdp']/BS1DF['at'] #profitability

BS1DF['DIVP'] = np.where(BS1DF['dvc'] > 0, 1, 0) #dividend payer
BS1DF['CASH'] = BS1DF['che']/BS1DF['at']
BS1DF['TANG'] = BS1DF['ppent']/BS1DF['at'] #tangf
BS1DF['CAPEX'] =  BS1DF['capx']/BS1DF['at']
BS1DF['ADVERT'] = BS1DF['xsga']/BS1DF['at']
BS1DF['RD'] = BS1DF['xrd']/BS1DF['at']

BS1DF['MLEV'] = BS1DF['TOTALDEBT_C']/(BS1DF['TOTALDEBT_C'] + BS1DF['MVEquity']) #tangf
BS1DF['BLEV'] = BS1DF['TOTALDEBT_C']/BS1DF['at']

list_variables = ['MVEquity','MVBook', 'PROF', 'DIVP','CASH', 'TANG', 'CAPEX', 'ADVERT', 'RD', 'MLEV', 'BLEV']


BS1DF.to_csv(os.path.join(datadirectory, "BSprocessedOCT19.csv"), index=False)

#for later: product uniqueness, CF volatility, asset maturity

COMPUCRSPIQCR = pd.merge(COMPUCRSPIQCR,
                   FUNDADEBT[c],
                    left_on=['gvkey','datadate'],
                    right_on = ['gvkey','datadate'], how='left')