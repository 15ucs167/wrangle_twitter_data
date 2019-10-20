import pandas as pd
import requests

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

def main():
    gather_data()

if __name__ == '__main__':
    main()
