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


def fama_french_ind(datadirectory, filename, industries = 48, nametosave='', option=0):
    """Takes a text file from KFrench website and returns dictionary with sic code as key and FF industry as value"""
    datadirectoryS = os.path.join(datadirectory, filename)
    with open(datadirectoryS) as f:
        lineList = f.read().splitlines()
    lineList = [i.strip() for i in lineList]
    onetofe = set(range(1, industries + 2))
    start = 0
    long_list = []
    for i in lineList:
        a = i.split(' ')[0]
        try:
            if int(a) in onetofe:
                short_list = []
                short_list.append(int(a))
                # start = 1
        except:
            start = 1
        if len(a) == 0 or a == '49':
            start = 0
            long_list.append(short_list)
        if start == 1:
            short_list.append(a)

    new_long_list = []
    count = 0
    for i in long_list:
        new_long_list.append([i[0]])
        for l in i[1:]:
            if int(l.split('-')[0]) == int(l.split('-')[1]):
                new_long_list[count].extend([int(l.split('-')[0])])
            else:
                new_long_list[count].extend(list(range(int(l.split('-')[0]), int(l.split('-')[1])+1)))
        count += 1
        #print(count)

    ff48_dict = {}
    for i in new_long_list:
        temp_list = [str(l).zfill(4) for l in i[1:]]

        for j in temp_list:
            ff48_dict.update({j: i[0]})
    #save to json file
    if option == 1:
        import json
        json.dump(ff48_dict, open(os.path.join(datadirectory, nametosave), 'w'))
    return ff48_dict



def winsor(data, column=[], quantiles=[0.99, 0.01]):
    """function to winsorize"""
    for i in column:
        print(i)
        data['temp1'] = data[i].quantile(quantiles[0])
        data['temp2'] = data[i].quantile(quantiles[1])
        new_var = i + '_cut'
        #data[new_var] = np.where(data[i] > data['temp1'], data['temp1'], data[i])
        data[new_var] = np.where(data[i] > data['temp1'], data['temp1'], data[i])
        data[new_var] = np.where(data[new_var] < data['temp2'], data['temp2'], data[new_var])
        #data[new_var] = np.where(data[i].isna, data[i], data[new_var])
        data = data.drop('temp1', axis=1)
        data = data.drop('temp2', axis=1)
    return data


def rol_vars(data, var, newname, group, onn, window, levels, group1=[], group2=[]):

    get_meth = getattr(data.groupby('gvkey').rolling(window, on=onn)[[var]], 'std')
    c = get_meth().reset_index()
    #print(c.shape)
    if levels == 2:
        c = pd.merge(c, data[group1],
                     left_on=group,
                     right_on=group, how='left')
        c = c.dropna(subset=[var])
        get_meth = getattr(c.groupby(group2)[[var]], 'mean')
        c = get_meth().reset_index()
    c.rename(columns={var: newname}, inplace=True)
    #print(c.shape)
    if levels == 2:
        data = pd.merge(data, c,
                          left_on = group2,
                          right_on = group2, how='left')
    if levels == 1:
        data = pd.merge(data, c,
                          left_on = group,
                          right_on = group, how='left')
    #print(data.shape)
    #del c
    return data, c


def match_closest(data1, data2, key1, date1, key2=0, date2=0, direction='backward'):
    if key2 == 0:
        key2 = key1
    if date2 == 0:
        date2 = date1
    data1['temp'] = '19400101'
    data2['temp'] = '19400101'
    data1['temp'] = pd.to_datetime(data1['temp'])
    data2['temp'] = pd.to_datetime(data2['temp'])
    data1['tempdays'] = (data1[date1] - data1['temp']).dt.days
    data2['tempdays'] = (data2[date2] - data2['temp']).dt.days

    data1[key1] = data1[key1].apply(int)
    data1[key1] = data1[key1].apply(str)
    data2[key2] = data2[key2].apply(int)
    data2[key2] = data2[key2].apply(str)
    data1['temp'] = '10000000'
    data2['temp'] = '10000000'
    data1['tempID'] = data1[key1] + data1['temp']
    data2['tempID'] = data2[key2] + data2['temp']
    data1['tempID'] = data1[key1].apply(int)
    data2['tempID'] = data2[key2].apply(int)
    data1['tempID'] = data1['tempID'] + data1['tempdays']
    data2['tempID'] = data2['tempID'] + data2['tempdays']

    data1 = data1.sort_values(by=['tempID'])
    data2 = data2.sort_values(by=['tempID'])

    data1 = pd.merge_asof(data1, data2, on='tempID', direction = direction)