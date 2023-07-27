'''
code for full-archive search - Twitter API v2
full archive search with Bearer token
per aAI request max 500 results; if there are more results, there is a next_token
each response gets appended to a csv

'''

import requests
import pandas as pd
import configparser
import json
import time
from datetime import datetime

path = "C:\\Users\\c.loschke\\Desktop\\Coding\\Twitter Diskursanalyse\\data"

# insert your own bearer token and cookie
# twitter
TWITTER_AUTH_TOKEN="Bearer AAAAAAAAAAAAAAAAAAAAAB%2F%2BjQEAAAAAeN2Mq4hJtZLv3M7G0aq8zatu8EQ%3D4emm2pbWWygcaFUdVUCVDWFuEqcTd20R3IkUjnghtA0krfzppM"
AUTH_TOKEN = TWITTER_AUTH_TOKEN
COOKIE = 'guest_id=v1%3A167483390163990556'

# change
QUERY = '''(((Energie OR Gas OR heizen OR Heizung OR Erdgas)
    (sparen OR spar OR spare OR spart OR sparst OR sparsam OR sparsame OR sparsames OR sparsamen OR sparsamer)) 
    OR 
    (Energiesparen OR Gassparen))
    -is:retweet
    '''

LANG = "de"
MAX_RESULT = 500 # 500 is max per API request
START_TIME = "2010-01-01T00:00:00Z"
END_TIME = "2023-04-10T00:00:00Z"

# ordered columns to keep from raw_tweets
COLUMN_ORDER = ["id", "url", "created_at", "author_id", "name", "username", "text", "possibly_sensitive", "conversation_id","reply_settings","in_reply_to_user_id","referenced_tweets","lang","edit_history_tweet_ids","public_metrics.retweet_count","public_metrics.reply_count","public_metrics.like_count","public_metrics.quote_count","public_metrics.impression_count","entities.mentions","entities.hashtags","entities.urls","attachments.media_keys","geo.place_id","geo.coordinates.type","geo.coordinates.coordinates","attachments.poll_ids","description","location","protected", "public_metrics.followers_count","public_metrics.following_count","public_metrics.tweet_count","public_metrics.listed_count" ,"verified","API_Query"]


# Attributes to scrape
TWEETS_FIELDS = "attachments,author_id,conversation_id,created_at,entities,geo,id,in_reply_to_user_id,lang,possibly_sensitive,public_metrics,referenced_tweets,reply_settings,source,text,withheld,edit_history_tweet_ids,edit_controls"
USER_FIELDS = "description,entities,location,name,protected,public_metrics,verified,withheld" # created_at is missing, because the term is already used in the tweet fields --> Workaround?
LIMIT = 2 # Number of API requests for testing
LIMITLESS = True # if True, the script will run until there are no more results

RETRY_ERRORS = [
    'Too Many Requests',
    'Service Unavailable'
]

def main(i, next_token):
    # response has next token - which is necessary for pagination (more than 500 results)
    has_next_token = True
    next_token = ""
    with open(f"{path}\\API_Tweets_append.csv", "a", encoding='utf-8-sig') as f:
        f.write(";".join(COLUMN_ORDER))
        f.write("\n")
    
    df_users = pd.DataFrame()
    df_tweets = pd.DataFrame()
    scraped_tweets = 0
    while has_next_token and (i <= LIMIT or LIMITLESS):
        response = API_call(next_token) # response is dictionary
        i += 1 # for each API request
         
        print(i)
        #if "next_token" in response["meta"]:
        if "meta" in response and "next_token" in response["meta"]:
            next_token = response["meta"]["next_token"]
        else:
            has_next_token = False
        # save latest data as json
        save_to_json(response)
        if 'title' in response and response['title'] in RETRY_ERRORS:
            print(f"---retry error {i} - will restart in 30 sec --- and time is {get_timestamp()}")
            time.sleep(30)
            main(i, next_token)
            break
        # make df
        df_users = pd.json_normalize(response["includes"]["users"])
        df_tweets = pd.json_normalize(response["data"])
        df_users.rename(columns={"id":"author_id"}, inplace=True)
        df = pd.merge(df_tweets, df_users, how='left', on="author_id")
        df["url"] = df.apply(lambda x: f'https://twitter.com/{x["username"]}/status/{x["id"]}', axis=1)

        # sort the columns in the desired order
        df = df.reindex(columns=COLUMN_ORDER)
        
        # fill coulumn Api_Query with query
        df["API_Query"] = QUERY
        # append each response to csv
        with open(f"{path}\\API_Tweets_append.csv", "a", encoding='utf-8-sig') as f:  
            df.to_csv(f,index=False, header=False, sep=';', line_terminator='\n')
        scraped_tweets += len(df_tweets)
    print("Number of Tweets scraped: "+ str(scraped_tweets))
    print(f"API Request done: {i-1}")
   



def API_call(token):
    if token == "":
        url = f"https://api.twitter.com/2/tweets/search/all?query={QUERY} lang:{LANG}&start_time={START_TIME}&end_time={END_TIME}&max_results={MAX_RESULT}&tweet.fields={TWEETS_FIELDS}&expansions=author_id&user.fields={USER_FIELDS}"
    else:
        url = f"https://api.twitter.com/2/tweets/search/all?query={QUERY} lang:{LANG}&start_time={START_TIME}&end_time={END_TIME}&max_results={MAX_RESULT}&next_token={token}&tweet.fields={TWEETS_FIELDS}&expansions=author_id&user.fields={USER_FIELDS}"
    
    #print(url)

    payload={}
    headers = {
    'Authorization': AUTH_TOKEN,
    'Cookie': COOKIE
    }
    response = requests.request("GET", url, headers=headers, data=payload)
    return response.json() # convert response to dictionary and return


'''
Todo: store error messages in txt file with timestamp

'''

def save_to_json(data):
    try: 
        with open(f"{path}\\data.json", "w") as file:
            json.dump(data, file)
    except Exception as e:
        print(e)
        print("could not save json")
        # dump as string
        with open(f"C:\\Coding\\Python\\Projects\\Twitter\\data\\data.txt", "w") as file:
            file.write(str(data))



def make_df(data):
    df_tweets = pd.json_normalize(data["tweets"])
    df_users = pd.json_normalize(data["users"]) # transforms a json dic into a nice df
    df_users = df_users.drop_duplicates(subset=["id"], keep="first") # remove duplicates
    df_users.rename(columns={"id":"author_id"}, inplace=True)
    return pd.merge(df_tweets, df_users, how='left', on="author_id")


def get_timestamp():
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")


if __name__ == '__main__':
    '''
    1. main
        2. API_call
            while loop to get all tweets (more than 500)
        3. make_df from json file
        4. append to csv
    '''
    print(f"------------------------------------------- start ------------------------------------------------- {get_timestamp()}")
    # start timer
    start_time = time.time()
    main(1, next_token="")
    print(f"------------------------------------------ end --------------------------------------------------- {get_timestamp()}")
    
    # print time it took to run the script
    elapsed_time = time.time() - start_time
    # Convert elapsed time to minutes and seconds
    minutes, seconds = divmod(elapsed_time, 60)
    # Check if elapsed time is more than 1 hour and convert it to hours and minutes if necessary
    if minutes >= 60:
        hours, minutes = divmod(minutes, 60)
        print("Time elapsed: %d hours %d minutes %d seconds" % (hours, minutes, seconds))
    else:
        print("Time elapsed: %d minutes %d seconds" % (minutes, seconds))
