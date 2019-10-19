import datetime, csv
#from sas7bdat import SAS7BDAT
import pandas as pd
import pandasql as ps
import numpy as np
import gzip, os, csv
import datetime

datadirectory = os.path.join(os.getcwd(), 'data')



FUNDADEBT = pd.read_csv(os.path.join(datadirectory, "fundadebt7019.txt.gz"), sep='\t')
FUNDADEBT['datadate'] = pd.to_datetime(FUNDADEBT['datadate'], format='%Y%m%d')
FUNDADEBT = FUNDADEBT[FUNDADEBT.indfmt == 'INDL']

FUNDACMP = pd.read_csv(os.path.join(datadirectory, "fundaCMP7019.gz"), sep='\t')
FUNDACMP = FUNDACMP[['gvkey' ,'datadate' ,'cmp']].dropna(subset=['cmp'])
FUNDACMP['datadate'] = pd.to_datetime(FUNDACMP['datadate'], format='%Y%m%d')

FUNDABS = pd.read_csv(os.path.join(datadirectory, "fundamainBS7018.gz"), sep='\t')
FUNDABS['datadate'] = pd.to_datetime(FUNDABS['datadate'], format='%Y%m%d')

FUNDAMAINID = pd.read_csv(os.path.join(datadirectory, "fundamainID7019.txt.gz"), sep='\t')
FUNDAMAINID = FUNDAMAINID[FUNDAMAINID.indfmt == 'INDL']

CRSPLINK = pd.read_csv(os.path.join(datadirectory, "CRSPLINK.gz"), sep='\t')
CAPIQID = pd.read_csv(os.path.join(datadirectory, "Capital IQ Identifier.txt"), sep='\t')

SPCR = pd.read_csv(os.path.join(datadirectory, "cpstCRN.txt"), sep='\t')
SPCR.drop(columns=['tic'])
SPCRNONA = SPCR.drop(columns=['tic']).dropna(subset=['splticrm'])