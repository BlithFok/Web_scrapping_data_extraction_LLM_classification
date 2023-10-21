# extract tweets dataset based on illness keywords occurences

import json
import pandas as pd
List = []
fin = open('tweets.json')
for i in fin:
  List.append(i)
List

# filter the dataset based on illness keywords and store in seperate dataframe

keywords = []
Likes = []
Tweets = []

for j in List:
  Len = len(j)
  j = j[0:len(j)-1]
  j = json.loads(j)


  keywords.append(j['keyword'])
  Likes.append(j['likes'])
  Tweets.append(j['tweet'])

  data = {
      "keywords": keywords,
      "likes": Likes,
      "tweets": Tweets
  }

df = pd.DataFrame(data)
df2 = df2.loc[(df['keywords'] == 'COVID-19')]
df2.to_csv('COVID-19_tweets.csv', index = False, encoding = 'UTF-8')
df3 = df3.loc[(df['keywords'] == 'COVID-19')]
df3.to_csv('COVID-19_tweets.csv', index = False, encoding = 'UTF-8')

