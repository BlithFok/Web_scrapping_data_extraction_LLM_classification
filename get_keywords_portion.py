# get the dataframe of percentage of posts that contain illness keyword on different days

import pandas as pd

data1001 = pd.read_csv('tweets_9000.csv')
col = data1001['created_at'].values.tolist()
for i in col:
  data1001.replace(i, i[0:10], inplace = True)
data1002 = data1001.sort_values(by = 'created_at')
col2 = data1002['created_at'].values.tolist()
Dict = dict(data1002['created_at'].value_counts())
List2 = []
List3 = []
List4 = []
for i in Dict:
  if i not in List2:
    x = 0
    data1003 = data1002.loc[(data1002['created_at'] == i)]
    data_tweet = data1003['tweet'].values.tolist()
    data_tweet2 = data1003['hashtags'].values.tolist()
    for j in disease:
      for k in range(len(data_tweet)):
        if (j in data_tweet[k] or j in data_tweet2[k]):
          if (x == 0):
            x = 1
            continue
          else:
            x += 1
    List2.append(str(x))
for i in Dict:
  if i not in List3:
    x = 0
    data1003 = data1002.loc[(data1002['created_at'] == i)]
    data_tweet = data1003['tweet'].values.tolist()
    data_tweet2 = data1003['hashtags'].values.tolist()
    for j in disease:
      for k in range(len(data_tweet)):
        if (j in data_tweet[k] or j in data_tweet2[k]):
          if (x == 0):
            x = 1
            continue
          else:
            x += 1
    List3.append(str(Dict[i]))
for i in Dict:
  if i not in List4:
    x = 0
    data1003 = data1002.loc[(data1002['created_at'] == i)]
    data_tweet = data1003['tweet'].values.tolist()
    data_tweet2 = data1003['hashtags'].values.tolist()
    for j in disease:
      for k in range(len(data_tweet)):
        if (j in data_tweet[k] or j in data_tweet2[k]):
          if (x == 0):
            x = 1
            continue
          else:
            x += 1
    List4.append(str(x/Dict[i]))
Fun = {'date': List, 'illness keywords matched': List2, 'number of posts': List3, 'precentage of keyword occurrence': List4}
S = pd.DataFrame.from_dict(Fun)
S.to_csv('relative_frequency_of_disease_keywords_on_posts_in_different_days.csv', index = False, encoding = 'UTF-8')
