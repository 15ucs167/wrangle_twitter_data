import pandas as pd
import requests
import tweepy
import json
import time

# Create a function for gathering data

def gather_data():

    # Put archive in a Pandas DataFrame
    archive = pd.read_csv('twitter-archive-enhanced.csv')

    # Download image predictions file from its URL using requests library
    url = 'https://d17h27t6h515a5.cloudfront.net/topher/2017/August/599fd2ad_image-predictions/image-predictions.tsv'
    response = requests.get(url)
    with open('image_predictions.tsv', mode='wb') as file:
        file.write(response.content)

    # Load the downloaded file in a Pandas Dataframe
    image_predictions = pd.read_csv('image_predictions.tsv', sep='\t')

    consumer_key = 'p8XPAcIYhPR6AlzJCJi1TCUcp'
    consumer_secret = 'uDjWIROWB6GFxStScSgRSAJkTdUObupJgNTRXq1kOLtztMZS4V'
    access_token = '4698250152-TjymcC5OjNnx7E7e4rGSHaYU6PoPgheIvQ2ICOK'
    access_secret = 'mTL9FF9C0a614gY7vhghWaVcXn0eEbcP3xi5Uc72E9qT3'

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)

    api = tweepy.API(auth)

    counter = 1

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
            continue

    tweet_list = []
    with open('tweet_json.txt', 'r') as f:
        for line in f:
            tweet_list.append(json.loads(line))

    tweet_data = pd.DataFrame(tweet_list)

def main():
    gather_data()

if __name__ == '__main__':
    main()
