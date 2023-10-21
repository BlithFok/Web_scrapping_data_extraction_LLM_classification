# get the percentage of illness related posts in different dates

import pandas as pd
data4 = pd.read_csv('Dataset_with_illness_3.csv')
data5 = data4.sort_values(by = 'date')
col3 = data5['date'].values.tolist()
for i in col3:
  data5.replace(i, i[0:10], inplace = True)
col = data5['date'].values.tolist()
col2 = data5['illness'].values.tolist()
Dict = {}
Dict2 = {}
for i in col:
  if i not in Dict:
    Dict[i] = 1
  else:
    Dict[i] = Dict[i] + 1
for i in Dict:
  if i not in Dict2:
    data100 = data5.loc[(data5['date'] == i) & (data5['illness'] != 'no illness')]
    x = len(data100.index)
    Dict2[i] = str(x) + '/' + str(int(Dict[i])) + ' = ' + str(x/int(Dict[i]))
Dict2

