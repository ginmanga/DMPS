### Add Compustat Balance Sheet Variables not kept in FUNABS
import pandas as pd
import numpy as np
import datetime
import os
import importlib
import Functions

data_all = os.path.join('C:\\', 'Users', 'Panqiao', 'Documents', 'Research', 'Data')
comp = os.path.join(data_all, 'Compustat')
inventories = pd.read_csv(os.path.join(comp, 'inventories', "Compustat-INVENTORIESAR.gz"), sep='\t')
FUNDAP = pd.read_csv(os.path.join(comp, "APWCAP5018.gz"), sep='\t')
FUNDABS = pd.read_csv(os.path.join(comp, "FUNDAMAINBS5018.gz"), sep='\t')
FUNDAP['datadate'] = pd.to_datetime(FUNDAP['datadate'], format='%Y%m%d')
inventories['datadate'] = pd.to_datetime(inventories['datadate'], format='%Y%m%d')

# Investories variables
# invfg - inventories finished goods
# invo - inventories other
# invrm - inventories raw materials
# invt - inventories - total
# invwip - inventories work in process
# rect receivable total
# rectr receivables trade