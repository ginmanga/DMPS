####Compile Execucomp Data nad add to main analysis file
import os
import pandas as pd
import numpy as np
data_all = os.path.join('C:\\', 'Users', 'Panqiao', 'Documents', 'Research', 'Data')
data_execo = os.path.join(data_all, 'execucomp')
acomp  = pd.read_csv(os.path.join(data_execo, 'Anual compensation', "annual compensation.gz"), sep='\t')