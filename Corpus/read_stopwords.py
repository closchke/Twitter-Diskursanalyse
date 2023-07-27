# read german stopword list from external .txt

def read_stopwords():    
    stopwords_file = 'stopwords_de.txt'
    stopwords = []

    with open(stopwords_file, "r", encoding='utf-8') as f:
        for line in f:
            stopwords.extend(line.split()) 

    # add more unwanted words to the existing stopwordlist
    unwanted_words = 'facebook reddit instagram twitter linkedIn socialmedia pinterest affiliat tumblr tiktok gt youtube dsl &amp &gt'
    list_unwanted_words = unwanted_words.split()
    stopwords += list_unwanted_words
    return stopwords

