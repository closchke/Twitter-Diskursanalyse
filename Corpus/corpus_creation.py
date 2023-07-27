'''
takes API_Tweets_append.csv and creates a corpus with several preprocessing steps:
    - renames columns
    - converts date to datetime
    - creates new id
    - creates new column with date_hour
    - converts content to string
    - deletes extensive lines
    - replaces html characters
    - creates new column with content_short (without links, @, extensive lines)
    - creates new df with spam accounts
    - creates new df with tweets with http
    - creates new df with regular tweets
    - creates new df with all tweets
    - saves all dfs to csv

'''


import pandas as pd
import regex as re
import datetime as dt

path = "C:\\Users\\c.loschke\\Desktop\\Coding\\Twitter Diskursanalyse\\data"

def corpus_creation():
    #tweets_df_raw = pd.read_feather(f'{path_feather}{name}.ft')
    tweets_df_raw = pd.read_csv(f'{path}\\API_Tweets_append.csv', sep=';', encoding='utf-8-sig')
    # rename columns
    tweets_df_raw.rename(columns={"created_at":"date", "text":"content"}, inplace=True)
    tweets_df_raw["date"] = pd.to_datetime(tweets_df_raw["date"])
    # raw data frame modification
    #tweets_df_raw["original_id"] = tweets_df_raw["id"] #.apply(str)

    #tweets_df_raw.drop(["id"], axis=1, inplace=True) # create new id, because working with original_id is not handy
    tweets_df_raw = tweets_df_raw.reset_index() # with reset the index, the old index is added as a column (named index), and a new sequential index is used; use (drop=True) to avoid adding the column
    tweets_df_raw = tweets_df_raw.rename(columns={"index":"id"})
    tweets_df_raw.rename(columns={"date":"datetime"}, inplace=True)
    tweets_df_raw['date'] = tweets_df_raw['datetime'].dt.strftime("%Y-%m-%d")
    tweets_df_raw["date_hour"] = tweets_df_raw['datetime'].dt.strftime("%Y-%m-%d %H")


    # Corpus modification
    tweets_df = tweets_df_raw

    # content as string
    tweets_df['content'] = tweets_df['content'].astype(str) # convert to string
    tweets_df['username'] = tweets_df['username'].astype(str) # convert to string
    
    lines_regex = re.compile(r'\s+') # one or more whitespaces (extensive lines)
    tweets_df['content'] = [re.sub(lines_regex, " ", tweet) for tweet in tweets_df['content']] # delete extensive lines

    # replace &amp; with &; &lt; with <; &gt; with >
    dict_html = {"&amp;": "&", "&lt;": "<", "&gt;": ">"}
    tweets_df["content"] = tweets_df["content"].replace(dict_html, regex=True)


    # content_short
    link_regex = re.compile(r'http[s]*\S+')
    tweets_df['content_short'] = [re.sub(link_regex, "", tweet) for tweet in tweets_df['content']] # create extra column for content without links

    www_regex = re.compile(r'www\S+') # delete www. and following
    tweets_df['content_short'] = [re.sub(www_regex, "", tweet) for tweet in tweets_df['content_short']]

    reaction_regex = re.compile(r'@\S+')
    tweets_df['content_short'] = [re.sub(reaction_regex, "", tweet) for tweet in tweets_df['content_short']]
    # count different users and create df
    user_count = tweets_df_raw['username'].value_counts().rename_axis('username').reset_index(name='counts') # value_counts creates series with unique users and their count; rename_axis and reset_index to format to df
    # safe user_count to csv
    user_count.to_csv(f'{path}\\user_count.csv', sep=';', index=False, encoding="utf-8-sig")

    # duplicates, sort by likes ascending, keep last
    d = tweets_df.sort_values('public_metrics.like_count').duplicated(subset=["content_short"], keep='last')
  
    duplicates_df = tweets_df[d] # creates df of duplicates

    '''Spam user: 
    1. Aggregate tweets by user and date and count the number of tweets per user
    2. create a list of users that have at least once more than 30 tweets per day
    3. count the number of users that have tweeted more than 10 times per day
    4. create a list of users that have tweeted more than 10 times per day for more than 3 days
    5. create a list of both (only unique users) with set()
    '''

    '''
    # 1.
    user_per_day = tweets_df.groupby(['date', 'username'])['id'].count().reset_index()
    user_per_day.rename(columns={"id":"count"}, inplace=True)
    # 2.
    user_with_more_than_30_tweets_per_day = user_per_day[user_per_day["count"] > 30] # filter for users with more than 10 tweets per day
    list_of_users_with_more_than_30_tweets_per_day = user_with_more_than_30_tweets_per_day["username"].unique().tolist()
    # 3. 
    user_10_tweets_per_day = user_per_day[user_per_day["count"] > 10] # filter for users with more than 10 tweets per day
    user_10_tweets_per_day = user_10_tweets_per_day.groupby(['username'])['count'].count().reset_index()
    # 4.
    user_10_tweets_per_day_3_days = user_10_tweets_per_day[user_10_tweets_per_day["count"] > 3] # filter for users with more than 10 tweets per day for more than 3 days
    list_of_users_with_more_than_10_tweets_per_day_3_days = user_10_tweets_per_day_3_days["username"].unique().tolist()
    # 5.
    list_of_spam_users = list(set(list_of_users_with_more_than_30_tweets_per_day + list_of_users_with_more_than_10_tweets_per_day_3_days))
    '''

    # manually identified spam accounts - if more spam should be (automatically) identified, use steps 1-4
    # add if run again: authent24039075 and maybe johnturnersn (92x RT and 127 false RT Tweets)
    list_of_spam_users = ["webfan47", "uijujui24", "MeinBesterTarif", "rs_deutschland", "Weltrettung69","power4cheap","Oekostrom1","dealsking_de"] # idetified by observing user_count, user_count_per_day and content (Weltrettung69 has more than 400 Tweets)
    spam_user = tweets_df[tweets_df.username.str.contains("wke_")]

    for username in list_of_spam_users:
        spam_user = pd.concat([spam_user, tweets_df[tweets_df.username == username]])


    spam_df = pd.concat([duplicates_df, spam_user]).drop_duplicates().sort_index()


    # remove tweets with George Orwell in content_short and add them to spam_df
    #george_orwell = tweets_df[tweets_df.content_short.str.contains("George Orwell", case=False) | tweets_df.content_short.str.contains("für den Krieg, der dem Frieden dient", case=False)]
    #spam_df = pd.concat([spam_df, george_orwell]).sort_index() 


    # remove all rows that are in spam_df from tweets_df
    tweets_df = tweets_df[~tweets_df.index.isin(spam_df.index)]

    # 
    http_df = tweets_df[tweets_df["content"].str.contains("http|www.")] # create df of tweets with http
    tweets_df = tweets_df[~tweets_df["content"].str.contains("http|www.")] # remove http tweets from tweets_df
    
    # add column identifying if row in spam or tweet df
    tweets_df["category"] = "regular"
    spam_df["category"] = "spam"
    http_df["category"] = "http"

    # create df with all tweets
    all_tweets_df = pd.concat([tweets_df, http_df]) # ignore_index=True ?

    # save modified dataframes to csv
    tweets_df.to_csv(f'{path}\\tweets_df.csv', sep=';', index=False, encoding="utf-8-sig")
    spam_df.to_csv(f'{path}\\spam_df.csv', sep=';', index=False, encoding="utf-8-sig")
    http_df.to_csv(f'{path}\\http_df.csv', sep=';', index=False, encoding="utf-8-sig")
    all_tweets_df.to_csv(f'{path}\\all_tweets_df.csv', sep=';', index=False, encoding="utf-8-sig")

    #tweets_df = tweets_df.reset_index(drop=True)
    #tweets_df.to_feather(f'{path_feather}tweets_df_{len(tweets_df)}_{now}.ft')
    # save as csv
    #tweets_df.to_csv(f'{path}tweets_df_{len(tweets_df)}_{now}.csv', sep=';', index=False, encoding="utf-8-sig", mode='x') # mode = x prevents from overwriting

    return http_df, spam_df, tweets_df