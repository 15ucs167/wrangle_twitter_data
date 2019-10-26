import pandas as pd
import requests
import tweepy
import json
import time
import matplotlib.pyplot as plt
import numpy as np

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
    """consumer_key =
    consumer_secret =
    access_token =
    access_secret = 

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

def clean_data(archive, image_predictions, tweet_data):

    # Create copies of DataFrames to work on
    archive_clean = archive.copy()
    image_predictions_clean = image_predictions.copy()
    tweet_data_clean = tweet_data.copy()

    # First drop unwanted columns
    archive_clean.drop(columns=['in_reply_to_user_id', 'source', 'retweeted_status_user_id',
     'retweeted_status_timestamp', 'expanded_urls'], inplace=True)
    image_predictions_clean.drop(columns=['jpg_url', 'img_num', 'p1_conf', 'p2', 'p2_conf', 'p2_dog', 'p3', 'p3_conf', 'p3_dog'], inplace=True)
    tweet_data_clean = tweet_data.iloc[:, [1, 20, 21]].copy()

    # Issue: Three tables instead of one for an observational unit

    # Define
    # Use merge() to join the three tables on tweet_id

    # Code
    archive_clean = archive_clean.merge(image_predictions_clean, how='left')
    # Change column name from id to tweet_id to merge with archive_clean
    tweet_data_clean.rename(columns={'id': 'tweet_id'}, inplace=True)
    archive_clean = archive_clean.merge(tweet_data_clean, how='left', on='tweet_id')

    # Test
    archive_clean


    # Issue: Dog type as different columns

    # Define
    # Use melt() to create column for dog type

    # Code
    archive_c = archive_clean.melt(id_vars=['tweet_id', 'in_reply_to_status_id', 'timestamp', 'text', 'retweeted_status_id', 'rating_numerator', 'rating_denominator', 'name', 'p1', 'p1_dog', 'retweet_count', 'favorite_count'], value_vars=['doggo', 'floofer', 'pupper', 'puppo'], var_name='dog_type', value_name='actual_dog_type')
    archive_c.drop(columns='dog_type', inplace=True)
    archive_c.duplicated()
    archive_c.duplicated().sum()
    archive_c.drop_duplicates(inplace=True)
    archive_clean = archive_c.copy()

    # Test
    archive_clean


    # Issue: some records are replies instead of tweets (invalid data) - Only one of the replies consists of relevant data (Tessa)

    # Define
    # Remove all records with non-null in_reply_to_status_id except where name is Tessa

    # Code
    archive_clean = archive_clean.query('in_reply_to_status_id == "NaN" or name == "Tessa"')

    # Test
    archive_clean
    archive_clean.query('name == "Tessa"')
    archive_clean.query('in_reply_to_status_id != "Nan"')


    # Issue: some records are just retweets (invalid data)

    # Define
    # Remove all records where the retweeted_status_id is not null

    #Code
    archive_clean = archive_clean.query('retweeted_status_id == "NaN"')

    # Test
    archive_clean.query('retweeted_status_id != "NaN"')

    # Now the in_reply_to_status_id and retweeted_status_id columns are no longer needed.
    archive_clean.drop(columns=['in_reply_to_status_id', 'retweeted_status_id'], inplace=True)


    # Issue: Some records have things other than dogs as their subject (invalid data)

    # Define
    # Remove records where p1_dog value is false based on our predictions

    #Code
    archive_clean = archive_clean.query('p1_dog == True')

    # Test
    archive_clean.query('p1_dog == False')

    # Now, p1_dog column is no longer needed.

    archive_clean.drop(columns=['p1_dog'], inplace=True)


    # Issue: Non-descriptive column name: p1

    # Define
    # Change p1 to predicted_breed

    # Code
    archive_clean.rename(columns={'p1': 'predicted_breed', 'actual_dog_type': 'dog_type'}, inplace=True)

    # Test
    archive_clean.head()
    archive_clean.info()


    # Issue: name = 'a' (inaccurate data)

    # Define
    # From text, extract the correct names

    # Code
    archive_clean.reset_index(drop=True, inplace=True)
    archive_clean.query('name == "a"')
    archive_clean['extract'] = archive_clean['text'].str.extract(r'(named [A-Za-z\(\)\s]+\s?\.)')
    archive_clean.query('name == "a"')
    archive_clean['extract'] = archive_clean['extract'].str[6:-1]
    archive_clean.query('name == "a"')
    archive_clean['extract'].value_counts()
    archive_clean.query('extract in ["Zeus", "Chuk", "Klint", "Jessiga", "Leroi", "Octaviath", "Wylie", "Guss", "Tickles", "Johm", "Alfonso", "Rufus", "Hemry", "Kohl", "Alfredo", "Cheryl", "Berta"]')['extract']
    archive_clean.iat[1151, 5] = 'Wylie'
    archive_clean.iat[1280, 5] = 'Rufus'
    archive_clean.iat[1317, 5] = 'Henry'
    archive_clean.iat[1335, 5] = 'Alfredo'
    archive_clean.iat[1340, 5] = 'Zeus'
    archive_clean.iat[1353, 5] = 'Leroi'
    archive_clean.iat[1362, 5] = 'Berta'
    archive_clean.iat[1371, 5] = 'Chuk'
    archive_clean.iat[1376, 5] = 'Guss'
    archive_clean.iat[1382, 5] = 'Alfonso'
    archive_clean.iat[1388, 5] = 'Cheryl'
    archive_clean.iat[1392, 5] = 'Jessica'
    archive_clean.iat[1396, 5] = 'Klint'
    archive_clean.iat[1400, 5] = 'Tickles'
    archive_clean.iat[1404, 5] = 'Kohl'
    archive_clean.iat[1431, 5] = 'Octaviath'
    archive_clean.iat[1433, 5] = 'John'

    # Test
    archive_clean.query('extract in ["Zeus", "Chuk", "Klint", "Jessiga", "Leroi", "Octaviath", "Wylie", "Guss", "Tickles", "Johm", "Alfonso", "Rufus", "Hemry", "Kohl", "Alfredo", "Cheryl", "Berta"]')


    # Issue: timestamp string instead of datetime (inaccurate data)

    # Define
    # Use to_datetime to convert dtype to datetime

    # Code
    archive_clean.drop(columns='extract', inplace=True)
    archive_clean['timestamp'] = pd.to_datetime(archive_clean['timestamp'], infer_datetime_format=True)

    # Test
    archive_clean.info()


    # Issue: dog type string instead of category type (inaccurate data)

    # Define
    # Convert to category dtype using astype()

    # Code
    archive_clean['dog_type'] = archive_clean['dog_type'].astype('category')

    # Test
    archive_clean.info()


    # Issue: rating_numerator is int even though some values are decimals (inaccurate data)

    # Define
    # Convert using astype()

    # Code
    archive_clean['rating_numerator'] = archive_clean['rating_numerator'].astype('float64')

    # Test
    archive_clean.info()


    # Issue: rating_numerator = 27 is actually 11.27 (inaccurate data)

    # Define
    # Correct using iat()

    # Code
    archive_clean.query('rating_numerator == 27')
    # We have duplicate rows (the only difference is that one has the dog_type pupper but the other doesn't)
    archive_clean.drop(index=448, inplace=True)
    archive_clean.reset_index(drop=True, inplace=True)
    archive_clean.query('rating_numerator == 27')
    archive_clean.iloc[1559]['text']
    archive_clean.iat[1559, 3] = 11.27

    # Test
    archive_clean.iloc[1559]


    # Issue: rating_numerator = 75 is actually 9.75 (inaccurate data)

    # Define
    # Correct using iat()

    # Code
    archive_clean.query('rating_numerator == 75')
    archive_clean.iloc[408]['text']
    archive_clean.iat[408, 3] = 9.75

    # Test
    archive_clean.iloc[408]


    return archive_clean

def store_data(archive_clean):
    archive_clean.to_csv('twitter_archive_master.csv', index=False)

def analyze_data():
    df = pd.read_csv('twitter_archive_master.csv')

    # Most popular dog names

    name_counts = df['name'].value_counts()[2: 13]
    name_counts
    name_counts.drop(index='the', inplace=True)
    plt.pie(name_counts, labels=name_counts.index );
    plt.title('MOST POPULAR DOG NAMES');
    plt.show()

    # Cooper is the most popular dog name

    # Most Popular Dog Breeds

    breed_count = df['predicted_breed'].value_counts()
    breed_count
    breed_count_proportions = breed_count/breed_count.sum()
    popular_breeds = breed_count_proportions[0: 10]
    ind = np.arange(len(popular_breeds))
    width = 0.35
    plt.bar(0 , popular_breeds.values[0], label=popular_breeds.index[0]);
    plt.bar(1, popular_breeds.values[1], label=popular_breeds.index[1]);
    plt.bar(2, popular_breeds.values[2], label=popular_breeds.index[2]);
    plt.bar(3, popular_breeds.values[3], label=popular_breeds.index[3]);
    plt.bar(4, popular_breeds.values[4], label=popular_breeds.index[4]);
    plt.bar(5, popular_breeds.values[5], label=popular_breeds.index[5]);
    plt.bar(6, popular_breeds.values[6], label=popular_breeds.index[6]);
    plt.bar(7, popular_breeds.values[7], label=popular_breeds.index[7]);
    plt.bar(8, popular_breeds.values[8], label=popular_breeds.index[8]);
    plt.bar(9, popular_breeds.values[9], label=popular_breeds.index[9]);
    plt.ylabel('PROPORTION')
    plt.xlabel('DOG BREED')
    plt.title('MOST POPULAR DOG BREEDS')
    locations = ind + width/2
    labels = ind + 1
    plt.xticks(locations, labels)
    plt.legend();
    plt.show()

    # Golden Retriever is the most popular dog breed

    # Dog type and favorite_count

    avg_likes_per_dog_type = df.groupby('dog_type').sum()['favorite_count']/df.groupby('dog_type').count()['tweet_id']
    avg_likes_per_dog_type[1:].plot(kind='bar');
    plt.title('AVERAGE LIKES PER DOG TYPE');
    plt.xlabel('DOG TYPE')
    plt.ylabel('AVG. NO. OF LIKES')
    plt.show()

    # Thus, we can see that out of the four dog types, puppo received the maximum likes while pupper received the least

def main():
    archive = gather_archive()
    image_predictions = gather_image_predictions()
    tweet_data = gather_additional_twitter_data(archive)
    assess_data(archive, image_predictions, tweet_data)
    archive_clean = clean_data(archive, image_predictions, tweet_data)
    store_data(archive_clean)
    analyze_data()

if __name__ == '__main__':
    main()
