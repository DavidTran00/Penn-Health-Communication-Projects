###########################################################################
###########################################################################
# Creates word frequency matrices and edge lists from survey responses 
# for further processing in network analysis
###########################################################################
###########################################################################

import itertools
import nltk
import os
import pandas as pd
import re
import spellchecker

# CONFIGURATION SETTINGS
INPUT_FILE = "C:/Users/Character Semantic Networks/interview_data.csv"
MATRIX_OUTPUT_FILE = "word_matrix.csv"
EDGE_OUTPUT_FILE = "edge_list.csv"
# character encoding of the input file
ENCODING = "ISO-8859-1"
# enable or disable various preprocessing steps
LEMMATIZE = False
REMOVE_STOPWORDS = True
SPELLCHECK = False
# discard all words shorter than MIN_WORD_LENGTH, set to 0 to keep all words
MIN_WORD_LENGTH = 3
# any suitable list of stop words can be substituted here
STOPWORDS = nltk.corpus.stopwords.words("english")

def get_wordnet_pos(treebank_tag):
    """ 
    translates Penn treebank part-of-speech tags to WordNet tags
    """
    if treebank_tag.startswith("J"):
        return nltk.corpus.wordnet.ADJ
    if treebank_tag.startswith("N"):
        return nltk.corpus.wordnet.NOUN
    if treebank_tag.startswith("R"):
        return nltk.corpus.wordnet.ADV
    if treebank_tag.startswith("V"):
        return nltk.corpus.wordnet.VERB
    return None
    
def preprocess(data):
    """
    various preprocessing steps to clean and prepare the text input
    """
    # convert all text to lower case
    data["Thought"] = data["Thought"].apply(lambda x: unicode(x).lower())
    # remove forward slashes which interfere with correct tokenization,
    # then tokenize text
    data["Thought"] = data["Thought"].str.replace('/', ' ')
    data["Thought"] = data["Thought"].apply(lambda x: nltk.word_tokenize(x))
    # remove numbers and any remaining punctuation marks from words
    # using regular expressions
    data["Thought"] = data["Thought"].apply(lambda x: 
        [re.sub(r"\d+", "", word) for word in x])
    data["Thought"] = data["Thought"].apply(lambda x: 
        [re.sub(r"[^\w\s]", "", word) for word in x]) 
    # attempt to correct misspelled words
    if SPELLCHECK:
        data["Thought"] = data["Thought"].apply(lambda x: 
            [spellchecker.correct(word) for word in x])
    # lemmatize the corpus to group together different inflections of words
    if LEMMATIZE:
        # apply part-of-speech tagging and covert from Penn to WordNet tags
        data["Thought"] = data["Thought"].apply(lambda x: nltk.pos_tag(x))
        data["Thought"] = data["Thought"].apply(lambda x: [(tt[0], 
             get_wordnet_pos(tt[1])) for tt in x])
        # lemmatize the pos-tagged corpus
        lemmatizer = nltk.stem.WordNetLemmatizer()
        data["Thought"] = data["Thought"].apply(lambda x: [
            lemmatizer.lemmatize(tt[0], tt[1]) if tt[1] is not None else 
            lemmatizer.lemmatize(tt[0]) for tt in x])
    # remove very short words
    data["Thought"] = data["Thought"].apply(lambda x: 
        [word for word in x if len(word) >= MIN_WORD_LENGTH])
    # remove stop words
    if REMOVE_STOPWORDS:
        data["Thought"] = data["Thought"].apply(lambda x: 
            [word for word in x if word not in STOPWORDS])
    return data
    
def create_edge_list():
    """
    creates list of edges in the word graph
    """
    # delete output file if it already exists
    try:
        os.remove(EDGE_OUTPUT_FILE)
    except OSError:
        pass
    data = pd.read_csv(INPUT_FILE, encoding=ENCODING)
    data = preprocess(data)
    surveys = data["Survey"].unique()
    for survey in surveys:
        survey_data = data["Thought"][data["Survey"] == survey]
        words = [word for thought in survey_data for word in thought]
        # get iterator over all two-word combinations
        combinations = [c for c in itertools.combinations(words, 2)]
        df = pd.DataFrame()
        df["Survey"] = [survey] * len(combinations)
        df["word1"] = [c[0] for c in combinations]
        df["word2"] = [c[1] for c in combinations]
        # append edge list of current survey to output file
        with open(EDGE_OUTPUT_FILE, "a") as f:
            df.to_csv(f, index=False, header=True if survey == 1 else False)

def create_word_matrix():
    """
    creates matrix of word frequencies
    """
    data = pd.read_csv(INPUT_FILE, encoding=ENCODING)
    data = preprocess(data)
    # create flat list of all words in the data set
    all_words = [word for thought in data["Thought"] for word in thought]
    # obtain the set of all unique words in the data
    distinct_words = set(all_words)
    # iterate over all distinct words and all survey responses, grouped by
    # survey number, and count word occurences
    surveys = data["Survey"].unique()
    word_counts = {}
    for word in distinct_words:
        counts = []
        for survey in surveys:
            survey_thoughts = data["Thought"][data["Survey"] == survey]
            # Flatten and filter the list to keep only instances of the 
            # current word
            survey_words = [w for thought in survey_thoughts for w in thought
                if w == word]
            counts.append(len(survey_words))
        word_counts[word] = counts
    # save results to data frame
    df = pd.DataFrame()
    for word in distinct_words:
        df[word] = word_counts[word]
    # use survey numbers as index of data frame
    df = df.set_index(surveys)
    df.index.name = "Survey"
    df.to_csv(MATRIX_OUTPUT_FILE, index=True, encoding=ENCODING)

def main():
    create_edge_list()
    create_word_matrix()

if __name__ == "__main__":
    main()
