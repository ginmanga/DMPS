#Calculate quarterly stadard deviations, by company and by SIC codes
#get SIC codes from CRSP monthly file


#load quarter data
#calculate quarterly (12) stdeviation of cash flow and sales per firm
# do it at industyr level use SIC codes Fama french 48 industy


FUNDAQ = pd.read_csv(os.path.join(datadirectory, "fundaQ.gz"), sep='\t')
FUNDAQ = pd.read_csv(os.path.join(datadirectory, "FUNDAQSICB2018.gz"), sep='\t')
FUNDASIC = pd.read_csv(os.path.join(datadirectory, "FUNDASICH.gz"), sep='\t')
FUNDAQ['datadate'] = pd.to_datetime(FUNDAQ['datadate'], format='%Y%m%d')
#FUNDADEBT = FUNDADEBT[FUNDADEBT.indfmt == 'INDL']



