# scrap reddit  information based on india locations

import praw
import pandas as pd

# Reddit App Credentials (replace with your own)
CLIENT_ID = my_client_id
CLIENT_SECRET = my_client_sercet
USER_AGENT = my_user_agent

# Authenticate with PRAW
reddit = praw.Reddit(
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    user_agent=USER_AGENT
)

# get the list of india cities

from indian_cities.dj_city import cities
import spacy
nlp = spacy.load("en_core_web_sm")

print(cities)
keywords1 = []
for i in cities:
  for j in i:
    try:
      keywords1.append(j)
      for k in j:
        try:
          keywords1.append(k)
          for m in k:
            try:
              keywords1.append(m)
            except:
              continue
        except:
          continue
    except:
      continue
  try:
    keywords1.append(i)
  except:
    continue

keywords = []
for i in keywords1:
  if len(i) == 1:
    continue
  elif (i == '(' or i == ')'):
    continue
  elif (type(i) == tuple):
    continue
  else:
    keywords.append(i)
keywords = list(set(keywords))
keywords.append('India')
keywords.append('Indian')
keywords2 = []
for i in range(len(keywords)):
  keywords[i] = keywords[i].lower()

print(keywords)
len(keywords)

# scrap data from reddit using praw and spacy nlp. Using spacy is to make sure that the matching is done correctly. Otherwise, 'indiagggy' will be classified as 'india' content



import spacy
import praw
nlp = spacy.load("en_core_web_sm")

posts = []
start_time = '2019-08-10'
end_time = '2021-09-10'

List = ["india", "indianews", "DoesAnybodyElse", "Advice", "needadvice", "medizzy", "medicine", "Health", "medicine", "Coronavirus", "COVID19", "PeopleFuckingDying", "flu"]
#List = ["Malaria", "Tuberculosis", "HIV", "hivaids", "hepatitis", "flu"]
for i in List:
  subreddit_name = i
  subreddit = reddit.subreddit(subreddit_name)
  for post in subreddit.top(limit=1000):
      Revalance = False
      if (i == 'india' or i == 'indianews'):
        Revalance = True
      date_created = post.created_utc
      location = 'india'
      title_location = []
      content_location = []
      location_1 = nlp(post.title)
      for entity in location_1.ents:
          if entity.label_ == "GPE":
              title_location.append(entity.text)
      location_2 = nlp(post.selftext)
      for entity in location_2.ents:
          if entity.label_ == "GPE":
              content_location.append(entity.text)
      for m in range(len(title_location)):
          title_location[m] = title_location[m].lower()
      for n in range(len(content_location)):
          content_location[n] = content_location[n].lower()
      for keyword in keywords:
          if (keyword in content_location or keyword in title_location):
              Revalance = True
              if (keyword != 'india' and keyword != 'indian'):
                location = keyword
      content = post.selftext
      if (Revalance == True):
        posts.append({
            'title': post.title,
            'content': post.selftext,
            'url': post.url,
            'date': date_created,
            'location': location
        })

df = pd.DataFrame(posts)
df['date'] = pd.to_datetime(df['date'], unit='s')

# the scraped data filtered based on time and whether the exact city location is provided or not

df2 = df.loc[ (df['date'] < end_time) & (df['date'] > start_time)]
df2.to_csv('dataset_location.csv', index = False, encoding = 'UTF-8')
df4 = df2.loc[ (df2['location'] != 'india') & (df2['location'] != 'indian')]
df4.to_csv('dataset_city_specified.csv', index = False, encoding = 'UTF-8')
df2
