#Calculate quarterly stadard deviations, by company and by SIC codes
#get SIC codes from CRSP monthly file


#load quarter data
#calculate quarterly (12) stdeviation of cash flow and sales per firm
# do it at industyr level use SIC codes Fama french 48 industy


FUNDAQ = pd.read_csv(os.path.join(datadirectory, "FUNDAQDEC4.txt"), sep='\t')

#FUNDADEBT = FUNDADEBT[FUNDADEBT.indfmt == 'INDL']
#calculate sale and cash flow volatility using quarterly data


