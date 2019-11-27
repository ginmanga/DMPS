#Calculate quarterly stadard deviations, by company and by SIC codes
#get SIC codes from CRSP monthly file


#load quarter data
#calculate quarterly (12) stdeviation of cash flow and sales per firm
# do it at industyr level use SIC codes Fama french 48 industy


FUNDAQ = pd.read_csv(os.path.join(datadirectory, "fundaQ.gz"), sep='\t')
FUNDAQ['datadate'] = pd.to_datetime(FUNDAQ['datadate'], format='%Y%m%d')
FUNDADEBT = FUNDADEBT[FUNDADEBT.indfmt == 'INDL']

COMPUCRSPIQCR = pd.read_csv(os.path.join(datadirectory, "MERGEDIDCR-ALLNOV27.csv"), index_col=0)
FUNDALIST_CRSPIDS = pd.read_csv(os.path.join(datadirectory, "FUNDALIST_CRSPIDSNOV27-ALLNOV27.csv"), index_col=0)