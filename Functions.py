import datetime, csv
#from sas7bdat import SAS7BDAT
import pandas as pd
import pandasql as ps
import numpy as np
import gzip, os, csv
import datetime
#want a function to get me a gvkey

def hhi_calculator(x, y, c, df):
    tt = [i + '_temp' for i in x]
    for i in x:
        df[i + '_temp'] = (df[i]/df[y])**2
    df[c] = df[tt].sum(axis=1)
    df[c] = (df[c]-(1/len(tt)))/(1-(1/len(tt)))
    return df.drop(tt, axis=1)

def pct_calculator(x, y, name, df):
    for i in x:
        df[i + name] = (df[i]/df[y])
    return df

def write_file(path_file, filename, data, write_type):
    """Writes all data to file"""
    #first tells if it writtes or appends
    #write_type 'w' or 'a'
    #print(data)
    path_to_2 = os.path.join(path_file, filename)
    with open(path_to_2, write_type, encoding='utf-8') as file:
        file.writelines('\t'.join(i) + '\n' for i in data)
    file.close()


def ext_fromzip(dir, filename, options=[]):
    acc_list = []
    with gzip.open(os.path.join(dir, filename), 'rt', encoding="utf8") as f:
        i = 0
        for line in f:
            listy = line.strip('\n').split('\t')
            # print(listy)
            # print(listy[1], listy[5])
            if i == 0:
                acc_list.append(listy)
            try:
                if int(listy[1]) in acc_list and (listy[5] == 'Annual') and (listy[7] == '10-K') and listy[17] != '':
                    acc_list.append(listy)
            except:
                None
            i += 1
    return acc_list



def collapse_list(a, x, y, header = 0):
    #a list to collapse
    #x group of variables used to collapase, indexes
    #y list of columns to keep
    # a collapsed list collpased with a 10K and AR path if any
    counter_1 = 0
    counter_2 = 0
    accumulator_text = []
    accumulator_doc = []
    accumulator_path = []
    if header == 0:
        collapsed_list = []
    else:
        collapsed_list = [[header[i] for i in y]]

    for i, item in enumerate(a):
        if counter_2 == 0:
            accumulator_text.append([a[i][ii] for ii in x])
            list_to_accumulate_doc_type = a[i][y[0]]
            list_to_accumulate_path = a[i][y[1]]

        if counter_2 > 0:
            list_to_check = [a[i][ii] for ii in x]
            list_to_accumulate_doc_type = a[i][y[0]]
            list_to_accumulate_path = a[i][y[1]]
            if list_to_check == accumulator_text[-1]:
                accumulator_text.append([a[i][ii] for ii in x])
                accumulator_doc.append(list_to_accumulate_doc_type)
                accumulator_path.append(list_to_accumulate_path)

            if list_to_check != accumulator_text[-1]:
                b = [accumulator_text[-1]]
                b.append(accumulator_doc)
                b.append(accumulator_path)
                collapsed_list.append(b)
                accumulator_text = [list_to_check]
                accumulator_doc = [list_to_accumulate_doc_type]
                accumulator_path = [list_to_accumulate_path]

        counter_2 += 1
    return collapsed_list


