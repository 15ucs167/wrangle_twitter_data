import pandas as pd
import requests
import tweepy
import json
import time

# Create a function for gathering data

def gather_archive():

    # Put archive in a Pandas DataFrame
    archive = pd.read_csv('twitter-archive-enhanced.csv')
    return archive

def gather_image_predictions():
    # Download image predictions file from its URL using requests library
    """url = 'https://d17h27t6h515a5.cloudfront.net/topher/2017/August/599fd2ad_image-predictions/image-predictions.tsv'
    response = requests.get(url)
    with open('image_predictions.tsv', mode='wb') as file:
        file.write(response.content)"""

    # Load the downloaded file in a Pandas Dataframe
    image_predictions = pd.read_csv('image_predictions.tsv', sep='\t')
    return image_predictions

def gather_additional_twitter_data(archive):
    #Create Twitter API object using tweepy
    """consumer_key = 'p8XPAcIYhPR6AlzJCJi1TCUcp'
    consumer_secret = 'uDjWIROWB6GFxStScSgRSAJkTdUObupJgNTRXq1kOLtztMZS4V'
    access_token = '4698250152-TjymcC5OjNnx7E7e4rGSHaYU6PoPgheIvQ2ICOK'
    access_secret = 'mTL9FF9C0a614gY7vhghWaVcXn0eEbcP3xi5Uc72E9qT3'

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)

    api = tweepy.API(auth)

    counter = 1

    #For each tweet in archive, gather additional data and store it in a txt file in a line
    for id in archive['tweet_id']:
        try:
            start = time.time()
            print(counter)
            print('Tweet ID: ', id)
            counter += 1
            tweet = api.get_status(id, tweet_mode='extended')
            with open('tweet_json.txt', 'a') as file:
                json.dump(tweet._json, file)
                file.write('\n')
                end = time.time()
                print('Time taken: ', end-start)

        except Exception as e:
            start = time.time()
            print(counter)
            print('Tweet ID: ', id)
            counter += 1
            end = time.time()
            print('Time taken', end-start)
            continue"""

    # For each line in the txt file, get the data as a Python dictionary, and append it to a list
    tweet_list = []
    with open('tweet_json.txt', 'r') as f:
        for line in f:
            tweet_list.append(json.loads(line))

    # Use the list of dictionaries to create a Pandas DataFrame
    tweet_data = pd.DataFrame(tweet_list)
    return tweet_data

def assess_data(archive, image_predictions, tweet_data):
    archive
    archive.iloc[5, 5]
    archive.info()
    archive['name'].value_counts()
    archive.query('name == "None"')
    archive.query('name == "a"')
    archive.query('rating_numerator < 10')
    archive.query('rating_numerator == 0')
    archive.query('rating_numerator == 1')
    archive.iloc[1016, 5]
    archive.iloc[315, 5]
    archive['rating_numerator'].value_counts()
    archive.query('rating_numerator == 420')
    archive.iloc[188, 5]
    archive.iloc[2074, 5]
    archive.query('rating_numerator == 75')
    archive.iloc[695, 5]
    archive.query('rating_numerator == 80')
    archive.iloc[1254, 5]
    archive.query('rating_numerator == 165')
    archive.iloc[902 ,5]
    archive.query('rating_numerator == 1776')
    archive.iloc[979, 5]
    archive.query('rating_numerator == 960')
    archive.iloc[313, 5]
    archive['retweeted_status_id'].value_counts()
    image_predictions
    image_predictions.info()
    tweet_data
    tweet_data.head()
    tweet_data.info()
    tweet_data['user']
    tweet_data['lang'].value_counts()
    tweet_data.query('lang == "und"')
    tweet_data.query('lang == "in"')
    tweet_data.query('lang == "nl"')
    tweet_data.query('lang  in ["es", "tl", "ro", "et"]')
    archive.head()
    archive.query('in_reply_to_status_id != "NaN"')
    archive.query('in_reply_to_status_id != "NaN" and name != "None"')
    archive.query('name == "Tessa"')
    archive.query('retweeted_status_id != "Nan"')
    archive.query('name == "Canela"')
    archive.duplicated().sum()
    archive['name'].isnull().sum()

    """Tidiness Issues

    1. Three tables instead of one for an observational unit
    2. Dog type as different columns


    Data Quality Issues

    1. Some records are replies instead of tweets (invalid data) - Only one of the replies consists of relevant data (Tessa)
    2. Some records are just retweets (invalid data)
    3. Some records have things other than dogs as their subject (invalid data)
    4. Non-descriptive column name: p1
    5. name = 'a' (inaccurate data)
    6. timestamp string instead of datetime (inaccurate data)
    7. dog_type string instead of category type (inaccurate data)
    8. rating_numerator is int even though some values are decimals (inaccurate data)
    9. rating_numerator = 27 is actually 11.27 (inaccurate data)
    10. rating_numerator = 75 is actually 9.75 (inaccurate data)"""

def main():
    archive = gather_archive()
    image_predictions = gather_image_predictions()
    tweet_data = gather_additional_twitter_data(archive)
    assess_data(archive, image_predictions, tweet_data)

if __name__ == '__main__':
    main()
