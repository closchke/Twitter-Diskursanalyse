'''
modifies the corpus with NLP methods for non machine learning analysis
    - tokenization
    - word count

'''


import regex as re
from nltk.tokenize import TweetTokenizer
from abbreviations import expand_abbreviations
from read_stopwords import read_stopwords
stopwords = read_stopwords()

from nltk.stem import WordNetLemmatizer
# Create a lemmatizer instance
lemmatizer = WordNetLemmatizer()


def tokenize_nltk(text):
    return TweetTokenizer().tokenize(text)

# Function to apply lemmatization to a list of tokens
def lemmatize_tokens(tokens):
    return [lemmatizer.lemmatize(token) for token in tokens]

def remove_stopwords(tokens):
    return [token for token in tokens if token not in stopwords]

def remove_query_words(tokens):
    return [token for token in tokens if token not in ['energie', 'gas', 'heizen', 'heizung', 'energiesparen', 'sparen', 'spar', 'sparsam', 'spart']]


def is_alpha_include_specific_words(token):
    include_pattern = r'^(CO2|65%|CO²)$' # insert new words here if needed
    if re.match(include_pattern, token):
        return True
    else:
        return token.isalnum()
        #return token.isalpha()



def concat_percent(tokens):
    import regex as re
    new_tokens = []
    i = 0
    while i < len(tokens):
        if i < len(tokens)-1 and re.match(r'^\d+%$', tokens[i] + tokens[i+1]):
            #new_tokens.append(tokens[i] + tokens[i+1])
            new_tokens.append(tokens[i] + "Prozent") # replace percent sign with word
            i += 2
        else:
            new_tokens.append(tokens[i])
            i += 1
    return new_tokens


# tokenization
def content_to_words(df):
    df['tokens_raw'] = df['content_short'].apply(tokenize_nltk)
    # count raw_tokens
    df['tokens_count'] = df['tokens_raw'].apply(len)
    # remove - and # from tokens
    df['tokens_mod'] = df['tokens_raw'].apply(lambda x: [token.replace("-", "").replace("#", "") for token in x])
    df['tokens_mod'] = df['tokens_mod'].apply(concat_percent)

    # lower tokens and only keep words
    df['words'] = df['tokens_mod'].apply(lambda x: [token.lower() for token in x if is_alpha_include_specific_words(token)])    # count words
    # count words
    df['words_count'] = df['words'].apply(len)
    # remove stopwords
    df['words_short']=df['words'].apply(remove_stopwords)
    # replace abbreviations (if list gets longer use abbreviations.py)
    # df['words_short'] = df['words_short'].apply(lambda x: [token.replace("akw", "atomkraftwerk").replace("akws", "atomkraftwerke") for token in x])
    # expand abbreviations
    df['words_short'] = df['words_short'].apply(expand_abbreviations)    
    # remove query words
    #df['words_short']=df['words_short'].apply(remove_query_words)
    # lemmatize

    return df