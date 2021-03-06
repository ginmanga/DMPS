import datetime, csv
#from sas7bdat import SAS7BDAT
import pandas as pd
import pandasql as ps
import numpy as np
import gzip, os, csv
import datetime
import importlib
import Functions
importlib.reload(Functions)
#want a function to get me a gvkey


def hhi_calculator(x, y, c, df, options=0, denominators=0, selection=0):
    tt = [i + '_temp' for i in x]
    if options == 1:
        if df[selection] == 0:
            y = denominators
    for i in x:
        df[i + '_temp'] = (df[i]/df[y])**2
    df[c] = df[tt].sum(axis=1)
    df[c] = (df[c]-(1/len(tt)))/(1-(1/len(tt)))
    return df.drop(tt, axis=1)


def pct_calculator(x, y, name, df, options=0, denominators=0, selection=0):
    if options == 1:
        if df[selection] == 0:
            y = denominators
    for i in x:
        df[i + name] = (df[i]/df[y])
    return df


def adj_dd1_old(data, list_vars, conditions=['NP_UNDER','NP_OVER','dd1']):
    for i in list_vars:
        var_name = i + 'PCT'
        print(var_name)
        data[i] = np.where((data[conditions[0]] == 1) & (data[conditions[2]] > 0),
                           data[i] + data[var_name]*data[conditions[2]], data[i])
        data[i] = np.where((data[conditions[1]] == 1) & (data[conditions[2]] > 0),
                                         data[i] - data[var_name]*data[conditions[2]], data[i])
    return data

def adj_dd1(data, list_vars, conditions=['NP_Exact', 'dd1']):
    for i in list_vars:
        var_name = i + 'PCT'
        data[i] = np.where((data[conditions[0]] == 1), data[i] + data[var_name] * data[conditions[1]], data[i])
    return data

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
    """What does this do?"""
    acc_list = []
    with gzip.open(os.path.join(dir, filename), 'rt', encoding="utf8") as f:
        i = 0
        for line in f:
            listy = line.strip('\n').split('\t')
            if i == 0:
                acc_list.append(listy)
            try:
                if int(listy[1]) in acc_list and (listy[5] == 'Annual') and (listy[7] == '10-K') and listy[17] != '':
                    acc_list.append(listy)
            except:
                None
            i += 1
    return acc_list


def collapse_list(a, x, y, header=0):
    # a list to collapse
    # x group of variables used to collapase, indexes
    # y list of columns to keep
    # a collapsed list collapsed with a 10K and AR path if any
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


def fama_french_ind(datadirectory, filename, industries=48, nametosave='', option=0):
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


def winsor(data, column=[], cond_list=[], cond_num=[], quantiles=[0.99, 0.01], year=1968, yearvar='fyear', freq='annual', options=1):
    """function to winsorize"""
    # print(year)
    # print(cond_list)
    # print(cond_num)
    # print(len(data))
    data_temp = data
    if options == 1:
        if freq == 'annual':
            data_temp = data[data[yearvar] >= year]
        if freq == 'qtr':
            data_temp = data[data['fyearq'] >= year]
        for index, elem in enumerate(cond_list):
            data_temp = data_temp[data_temp[elem] == cond_num[index]]
            print(len(data_temp))

    for i in column:
        print(i)
        data['temp1'] = data_temp[i].quantile(quantiles[0])
        data['temp2'] = data_temp[i].quantile(quantiles[1])
        print(data_temp[i].describe())
        new_var = i + '_cut'
        data[new_var] = np.where(data[i] > data['temp1'], data['temp1'], data[i])
        data[new_var] = np.where(data[new_var] < data['temp2'], data['temp2'], data[new_var])
        data = data.drop('temp1', axis=1)
        data = data.drop('temp2', axis=1)
    return data


def rol_vars(data, var, newname, group, onn, window, levels, mp1=None, group1=[], group2=[], mp=3):
    """Calculates rolling statistic given parameters"""
    if levels == 1:
        get_meth = getattr(data.groupby('gvkey').rolling(window, min_periods=mp1, on=onn)[[var]], 'std')
    if levels == 2:
        get_meth = getattr(data.groupby('gvkey').rolling(window, min_periods=mp, on=onn)[[var]], 'std')
    c = get_meth().reset_index()
    print(c.columns)
    if levels == 2:
        c = winsor(c, column=[var], cond_list=[], cond_num=[], quantiles=[0.975, 0.025], options=0)
        print(c[var + '_cut'].describe())
        c = pd.merge(c, data[group1],
                     left_on=group,
                     right_on=group, how='left')
        c = c.dropna(subset=[var])
        # get_meth = getattr(c.groupby(group2)[[var]], 'mean')
        get_meth = getattr(c.groupby(group2)[[var + '_cut']], 'mean')
        c = get_meth().reset_index()
    #c.rename(columns={var: newname}, inplace=True)
    c.rename(columns={var + '_cut': newname}, inplace=True)
    groups = [newname, 'FF48', 'fyear']
    if levels == 2:
        data = pd.merge(data, c[groups], left_on=group2, right_on=group2, how='left')
    if levels == 1:
        data = pd.merge(data, c, left_on=group, right_on=group, how='left')
    return data, c


def match_closest(data1, data2, key1, date1, key2=0, date2=0, direction='backward'):
    if key2 == 0:
        key2 = key1
    if date2 == 0:
        date2 = date1
    print(key1, key2, date1, date2)
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

    data1['tempID'] = data1['tempID'].apply(int)
    data2['tempID'] = data2['tempID'].apply(int)
    data1['tempID'] = data1['tempID'] + data1['tempdays']
    data2['tempID'] = data2['tempID'] + data2['tempdays']

    data1 = data1.sort_values(by=['tempID'])
    data2 = data2.sort_values(by=['tempID'])
    data1 = data1.drop(columns=['temp', 'tempdays'])
    data2 = data2.drop(columns=['temp', 'tempdays', key2, date2])

    data1 = pd.merge_asof(data1, data2, on=['tempID'], direction = direction)
    data1 = data1.drop(columns=['tempID'])
    return data1


def fin_ratio(data, ratios, denominator, names=[]):
    "takes data, a list of variables, names and a denomiator to calculate ratios"
    for index, elem in enumerate(ratios):
        data[names[index]] = data[elem]/data[denominator]
    return data


def rating_grps(data):
    ratings = ['AAA+', 'AAA', 'AAA-', 'AA+', 'AA', 'AA-', 'A+', 'A', 'A-', 'A-', 'BBB+', 'BBB', 'BBB-',
               'BB+', 'BB', 'BB-', 'B+', 'B', 'B-', 'CCC+', 'CCC', 'CCC-', 'CC+', 'CC', 'CC-', 'C']
    ig = ['AAA+', 'AAA', 'AAA-', 'AA+', 'AA', 'AA-', 'A+', 'A', 'A-', 'BBB+', 'BBB', 'BBB-']
    hig = ['AAA+', 'AAA', 'AAA-', 'AA+', 'AA', 'AA-']
    lig = ['A+', 'A', 'A-', 'A-''BBB+', 'BBB', 'BBB-']
    hjunk = ['BB+', 'BB', 'BB-']
    lj = ['B+', 'B', 'B-', 'CCC+', 'CCC', 'CCC-', 'CC+', 'CC', 'CC-', 'C']
    junk = ['BB+', 'BB', 'BB-', 'B+', 'B', 'B-', 'CCC+', 'CCC', 'CCC-', 'CC+', 'CC', 'CC-', 'C']
    D = ['D', 'SD']
    rated = ['rated', 'INVGRADE', 'JUNK', 'HJUNK', 'LJUNK', 'HIG', 'LIG', 'D']

    AAA = ['AAA+', 'AAA', 'AAA-']
    AA = ['AA+', 'AA', 'AA-']
    A = ['A+', 'A', 'A-']
    BBB = ['BBB+', 'BBB', 'BBB-']
    BB = ['BB+', 'BB', 'BB-']
    B = ['B+', 'B', 'B-']
    C = ['CCC+', 'CCC', 'CCC-', 'CC+', 'CC', 'CC-', 'C']
    groups = [ratings, ig, hig, lig, junk, hjunk, lj, AAA, AA, A, BBB, BB, B, C, D]
    names = ['RATED', 'INVGRADE', 'HIG', 'LIG', 'JUNK', 'HJUNK', 'LJUNK', 'AAA', 'AA', 'A', 'BBB', 'BB', 'B', 'C', 'D']

    for index, elem in enumerate(names):
        print(elem)
        conds = [data['splticrm'].isin(groups[index]), data['splticrm'].isnull()]
        choices = [1, np.nan]
        print(len(conds), len(choices))
        data[elem] = np.select(conds, choices, default=0)
    return data


def prep_asof(data, date, id, options=0):
    data['temp'] = '19600101'
    data['temp'] = pd.to_datetime(data['temp'])
    #data['date'] = pd.to_datetime(data[date], format='%Y%m%d')
    data['tempdays'] = (data[date] - data['temp']).dt.days
    if options == 0:
        data[id] = data[id].apply(int)
    data[id] = data[id].apply(str)
    data['temp'] = '10000000'
    data['tempID'] = data[id] + data['temp']
    data['tempID'] = data['tempID'].apply(int)
    data['tempID'] = data['tempID'] + data['tempdays']
    return data


def create_groups(data, grpby_var, var_name, new_name, grps=5):
    """Creates quintiles and groups"""
    # first create the quintiles
    # Then create the variables in the data frame
    quintile_holder = []
    for i in range(grps):
        newname = new_name + "_" + str(i+1)
        q = (1/grps) + i*(1/grps)
        data[newname] = 0
        newname = data.groupby([grpby_var])[[var_name]].quantile(q)
        quintile_holder.append(newname)
    data = data.apply(lambda row: sort_g(row, grpby_var, quintile_holder, var_name, new_name, grps), axis=1)
    return data

def sort_g(x, grpby_var, a, var, initial, grps=5):
    # print(a)
    for i in range(grps):
        #print(i)
        #print(a[i])
        if i == 0:
            # x['m'] = a[i].loc[x[grpby_var]][0]
            if x[var] <= a[i].loc[x[grpby_var]][0]:
                x[initial + '_1'] = 1
        if i > 0:
            # x['mm'] = a[i].loc[x[grpby_var]][0]
            if a[i-1].loc[x[grpby_var]][0] < x[var] <= a[i].loc[x[grpby_var]][0]:
                name = initial + "_" + str(i+1)
                x[name] = 1
    return x