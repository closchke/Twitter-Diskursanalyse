



def slice_df_by_topic(df, topic):
    """Slice a dataframe by topic.
    Args:
        df (pd.DataFrame): The dataframe to slice.
        topic (list): list of words to slice the df['words'] column by.
    Returns:
        pd.DataFrame: The sliced dataframe.
    """
    # create new dataframe containing the search_words
    mask = df['words'].apply(lambda x: any(word in x for word in topic))
    slice_df_search_words = df[mask].reset_index(drop=True)
    return slice_df_search_words



if __name__ == "__main__":
    # load data
    from read_data import read_feather_to_df
    tweets_df = read_feather_to_df('corpus_tweets_df')

    # slice dataframe by topic
    topic = ['krieg', 'putin', 'russland']
    slice_df = slice_df_by_topic(tweets_df, topic)

    # save sliced dataframe
    slice_df.to_csv('data/slice.csv', index=False)

    print("Done")