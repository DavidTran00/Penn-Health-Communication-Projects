##########################################################
##########################################################
# Run data checks and basic analyses of Twitter data 
# in large CSV files sent from collaborators
##########################################################
##########################################################

import datetime
import pandas as pd
import os 

input_dir = 'C:/Users/Twitter-data/all_tobacco_2014'
os.chdir(input_dir)

month_names = {"January": 1, "February": 2, "March": 3, "April": 4, "May": 5,
               "June": 6, "July": 7, "August": 8, "September": 9, 
               "October": 10, "November": 11, "December": 12}

def get_chunk_generator(filename, header, chunksize=5000):
    """
    creates a generator for the data to avoid 
    having to load the entire file into memory    
    
    filename (string): name of input file
    header (boolean): column names in first line?
    chunksize (integer): number of lines per chunk
    
    returns a generator for the chunks of data in a CSV file 
    (as a pandas dataframe)
    """
    with open(filename) as csvfile:
        columns = csvfile.readline().strip().split(',')
    reader = pd.read_csv(filename, chunksize=chunksize, iterator=True, 
                         skiprows=1 if header else 0, header=None)    
    while True:
        try:
            # reader throws exception when end of file is reached
            chunk = reader.get_chunk()
        except:
            break
        chunk.columns = columns
        yield chunk

def get_date_range(filename):
    """
    finds the earliest and latest date of any tweets in the input file
    returns a pair of datetime dates: (min date, max date)
    """
    min_date = datetime.date(datetime.MAXYEAR, 1, 1)
    max_date = datetime.date(datetime.MINYEAR, 1, 1)

    chunk_generator = get_chunk_generator(filename, header=True)
    for chunk in chunk_generator:
        chunk["Month"] = chunk["Month"].replace(month_names)
        chunk["Date"] = chunk.apply(lambda row: 
            datetime.date(row["Year"], row["Month"], row["Day"]), axis=1)
        chunk_min_date = chunk["Date"].min()
        chunk_max_date = chunk["Date"].max()
        if chunk_min_date < min_date:
            min_date = chunk_min_date
        if chunk_max_date > max_date:
            max_date = chunk_max_date
    return (min_date, max_date)
    
def get_num_unique_tweets(filename):
    """
    returns the number of unique tweet ids (column "Idpost" in CSV file)
    """
    tweet_ids = []
    chunk_generator = get_chunk_generator(filename, header=True)
    for chunk in chunk_generator:
        ids = chunk["Idpost"].unique()
        for tweet_id in ids:
            if tweet_id not in tweet_ids:
                tweet_ids.append(tweet_id)
    return len(tweet_ids)
    
def get_duplicate_tweet_ids(filename):
    """
    returns list of duplicate tweet ids (column "Idpost")
    """
    tweet_ids = []
    duplicate_tweet_ids = []
    chunk_generator = get_chunk_generator(filename, header=True)
    for chunk in chunk_generator:
        ids = chunk["Idpost"]
        for tweet_id in ids:
            if tweet_id in tweet_ids:
               duplicate_tweet_ids.append(tweet_id)
            else:
                tweet_ids.append(tweet_id)
    return duplicate_tweet_ids

def main():
    print get_num_unique_tweets("201406.csv")
    date_range = get_date_range("201406.csv")
    print date_range[0], date_range[1]
    print get_duplicate_tweet_ids("201406.csv")

if __name__ == "__main__":
    main()
