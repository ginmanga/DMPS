import pandas as pd
import numpy as np
import csv
import datetime
import os
import importlib
import Functions
importlib.reload(Functions)


datadirectory = os.path.join(os.getcwd(), 'data')
data_all = os.path.join('C:\\', 'Users', 'Panqiao', 'Documents', 'Research', 'Data')

FUNDADEBT = pd.read_csv(os.path.join(datadirectory, "FUNDADEBT19502018.gz"), sep='\t')
FUNDADEBT['datadate'] = pd.to_datetime(FUNDADEBT['datadate'], format='%Y%m%d')
FUNDADEBT = FUNDADEBT[FUNDADEBT.indfmt == 'INDL']

FUNDADEBT = FUNDADEBT.loc[:, ~FUNDADEBT.columns.str.endswith('_fn')]
FUNDADEBT = FUNDADEBT.loc[:, ~FUNDADEBT.columns.str.endswith('_dc')]

FUNDACMP = pd.read_csv(os.path.join(datadirectory, "fundaCMP7019.gz"), sep='\t')
FUNDACMP = FUNDACMP[['gvkey', 'datadate', 'cmp']].dropna(subset=['cmp'])
FUNDACMP['datadate'] = pd.to_datetime(FUNDACMP['datadate'], format='%Y%m%d')

FUNDABS = pd.read_csv(os.path.join(datadirectory, "FUNDAMAINBS5018.gz"), sep='\t')
FUNDACSHPRI = pd.read_csv(os.path.join(datadirectory, "FUNDACSHPRI5018.gz"), sep='\t')
FUNDABS['datadate'] = pd.to_datetime(FUNDABS['datadate'], format='%Y%m%d')
FUNDACSHPRI['datadate'] = pd.to_datetime(FUNDACSHPRI['datadate'], format='%Y%m%d')
FUNDABS = pd.merge(FUNDABS, FUNDACSHPRI[['gvkey', 'datadate', 'cshpri']], left_on=['gvkey', 'datadate'],
                   right_on=['gvkey', 'datadate'], how='left')
FUNDABS2 = FUNDABS.loc[:, ~FUNDABS.columns.str.endswith('_dc')]
FUNDABS2 = FUNDABS2.loc[:, ~FUNDABS2.columns.str.endswith('_fn')]
FUNDABS = FUNDABS2
del FUNDABS2

# add chspri

FUNDAP = pd.read_csv(os.path.join(datadirectory, "APWCAP5018.gz"), sep='\t')
FUNDAP['datadate'] = pd.to_datetime(FUNDABS['datadate'], format='%Y%m%d')

# FUNDAMAINID = pd.read_csv(os.path.join(datadirectory, "fundamainID7019.txt.gz"), sep='\t')
FUNDAMAINID = pd.read_csv(os.path.join(datadirectory, "FUNDAMAINID5018.gz"), sep='\t')
FUNDAMAINID = FUNDAMAINID[FUNDAMAINID.indfmt == 'INDL']

CRSPLINK = pd.read_csv(os.path.join(datadirectory, "CRSPLINK.gz"), sep='\t')
CAPIQID = pd.read_csv(os.path.join(datadirectory, "Capital IQ Identifier.txt"), sep='\t')

#Data CRSP
data_all = os.path.join('C:\\', 'Users', 'Panqiao', 'Documents', 'Research', 'Data')
CRSP = os.path.join(data_all, 'Compustat', 'CRSP')

CRSPM = pd.read_csv(os.path.join(CRSP, "CRSPMONTHLY5018.gz"), sep='\t')
CRSPM['date'] = pd.to_datetime(CRSPM['date'], format='%Y%m%d')

SPCR = pd.read_csv(os.path.join(datadirectory, "cpstCRN.txt"), sep='\t')
SPCR.drop(columns=['tic'])
SPCRNONA = SPCR.drop(columns=['tic']).dropna(subset=['splticrm'])

CRSPM["COMNAM"]

CRSPM.loc[CRSPM['COMNAM'] == 'A P L CORP']
