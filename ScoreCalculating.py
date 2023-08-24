import gc
import re
import pandas as pd
import dask.dataframe as dd
import tqdm


# helpers func
def load_transcripts(file):
    transcript = pd.read_pickle(file, compression='zip')
    transcript['componenttext'] = transcript['componenttext'].astype(str)
    transcript['transcriptid'] = transcript['transcriptid'].astype(str)
    transcript = transcript.groupby('transcriptid').componenttext.apply(lambda x: ' '.join(x))
    transcript = transcript.reset_index(0)
    transcript.columns = ['title', 'text']
    # print(transcript)

    transcript_1, transcript_2, transcript_3, transcript_4 = dd.from_pandas(transcript, 1).random_split(
        [0.25, 0.25, 0.25, 0.25])
    transcript_1 = transcript_1.compute()
    transcript_2 = transcript_2.compute()
    transcript_3 = transcript_3.compute()
    transcript_4 = transcript_4.compute()

    transcript_dict_1 = {}
    for index, row in transcript_1.iterrows():
        transcript_dict_1[row['title']] = {'text': row['text']}

    transcript_dict_2 = {}
    for index, row in transcript_2.iterrows():
        transcript_dict_2[row['title']] = {'text': row['text']}

    transcript_dict_3 = {}
    for index, row in transcript_3.iterrows():
        transcript_dict_3[row['title']] = {'text': row['text']}

    transcript_dict_4 = {}
    for index, row in transcript_4.iterrows():
        transcript_dict_4[row['title']] = {'text': row['text']}

    return transcript_dict_1, transcript_dict_2, transcript_dict_3, transcript_dict_4


def load_words():
    sentimentwords_file = './risk_exposure/05_political_and_COVID_bigrams/firmlevelrisk-master/input/sentimentwords/LoughranMcDonald_MasterDictionary_2018.csv'
    riskwords_file = './risk_exposure/05_political_and_COVID_bigrams/firmlevelrisk-master/input/riskwords/synonyms.txt'
    polbigrams_file = './risk_exposure/05_political_and_COVID_bigrams/firmlevelrisk-master/input/political_bigrams/political_bigrams.csv'
    # Import positive and negative sentiment words, risk words, and collect all
    sentiment_words = import_sentimentwords(sentimentwords_file)
    risk_words = import_riskwords(riskwords_file)
    allwords = dict(sentiment_words, **{'risk': risk_words})
    # Import political bigrams
    political_bigrams = import_politicalbigrams(polbigrams_file)
    # SarsCov2-related words
    sarscov2 = ['Coronavirus', 'Corona virus', 'coronavirus',
                'Covid-19', 'COVID-19', 'Covid19', 'COVID19',
                'SARS-CoV-2', '2019-nCoV']
    sarscov2_words = set([re.sub('[^a-z ]', '', x.lower()) for x in sarscov2])

    return allwords, sarscov2_words, political_bigrams


def preprocess(nested_dict, window_size=20):
    # Make copy into which window of 22 words is pasted
    result = nested_dict.copy()

    # Loop
    for title, content in tqdm.tqdm(nested_dict.items(), desc='Preprocessing'):
        # print('Preprocessing on transcript_id:', title)
        # Access raw text
        text_str = content['text']

        # Preprocess
        text_str = re.sub(r'[^a-zA-Z ]', '', text_str.lower())
        words = text_str.split()

        # Bigrams
        bigrams = [' '.join(x) for x in zip(words[0:], words[1:])]

        # Window of +/- 10 consecutive bigrams
        window = list(zip(*[bigrams[i:] for i in range(window_size + 1)]))

        result[title]['bigram_windows'] = window
        result[title]['cleaned'] = words

    return result


def import_sentimentwords(file):
    df = pd.read_csv(file, sep=',')
    tokeep = ['Word', 'Positive']
    positive = set([x['Word'].lower() for idx, x in df[tokeep].iterrows()
                    if x['Positive'] > 0])
    tokeep = ['Word', 'Negative']
    negative = set([x['Word'].lower() for idx, x in df[tokeep].iterrows()
                    if x['Negative'] > 0])
    return {'positive': positive, 'negative': negative}


def import_riskwords(file):
    synonyms = set()
    with open(file, 'r') as inp:
        for line in inp:
            split = line.split(' ')
            for syn in split:
                synonyms.add(re.sub('\n', '', syn))
    return synonyms


def import_politicalbigrams(file):
    df = pd.read_csv(file, sep=',', encoding='utf-8')
    df = df.assign(bigram=df['bigram'].str.replace('_', ' '))
    df.rename(columns={'politicaltbb': 'tfidf'}, inplace=True)
    df.set_index('bigram', inplace=True)
    return df.to_dict(orient='index')


def calculating_scores(preprocessed):
    scores = {}
    for title, content in tqdm.tqdm(preprocessed.items(), desc='Calculating'):

        # print('Calculating on transcript_id:', title)
        scores[title] = {}

        # Access preprocessed windows of consecutive bigrams
        windows = content['bigram_windows']
        words = content['cleaned']

        # Total number of words (to normalize scores)
        totalwords = len(words)

        ### A) Score unconditional scores
        risk = len([word for word in words if word in allwords['risk']])
        sentpos = len([word for word in words if word in allwords['positive']])
        sentneg = len([word for word in words if word in allwords['negative']])
        covid = len([word for word in words if word in sarscov2_words])

        # Collect and prepare for conditional scores
        scores[title] = {
            'Risk': risk,
            'Sentiment': sentpos - sentneg,
            'Covid': covid,
            'Pol': 0,
            'PRisk': 0,
            'PSentiment': 0,
            'Total words': totalwords
        }

        ### B) Score conditional scores
        # Loop through each windows
        for window in windows:

            # Find middle ngram and check whether a "political" bigram
            middle_bigram = window[10]
            if middle_bigram not in political_bigrams:
                continue
            tfidf = political_bigrams[middle_bigram]['tfidf']

            # Create word list for easy and quick access
            window_words = set([y for x in window for y in x.split()])

            # If yes, check whether risk synonym in window
            conditional_risk = (len([word for word in window_words
                                     if word in allwords['risk']]) > 0)

            # If yes, check whether positive or negative sentiment
            conditional_sentpos = len([word for word in window_words
                                       if word in allwords['positive']])
            conditional_sentneg = len([word for word in window_words
                                       if word in allwords['negative']])

            # Weigh by tfidf
            conditional_risk = conditional_risk * tfidf
            conditional_sentpos = conditional_sentpos * tfidf
            conditional_sentneg = conditional_sentneg * tfidf

            # Collect results
            scores[title]['Pol'] += tfidf
            scores[title]['PRisk'] += conditional_risk
            scores[title]['PSentiment'] += (conditional_sentpos - conditional_sentneg)

    # Collect in dataframe
    scores_df = pd.DataFrame().from_dict(scores, orient='index')
    scores_df.index.name = 'transcriptid'

    # Scale
    toscale = [x for x in scores_df.columns if x not in {'Total words'}]
    for column in toscale:
        scores_df[column] = scores_df[column] * 100000 * (1 / scores_df['Total words'])

    return scores_df


file_list = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18',
             '19', '20', 'x']
file_path = './transcripts/ciqtranscriptcomponent_'

for file in file_list:
    print('Working on file: ' + file)
    transcript_raw_1, transcript_raw_2, transcript_raw_3, transcript_raw_4 = load_transcripts(file_path + file + '.pkl')
    allwords, sarscov2_words, political_bigrams = load_words()

    print('File ' + file + ' partition_1:')
    transcript_preprocessed = preprocess(transcript_raw_1)
    res_1 = calculating_scores(transcript_preprocessed)
    del transcript_preprocessed, transcript_raw_1
    gc.collect()

    print('File ' + file + ' partition_2:')
    transcript_preprocessed = preprocess(transcript_raw_2)
    res_2 = calculating_scores(transcript_preprocessed)
    del transcript_preprocessed, transcript_raw_2
    gc.collect()

    print('File ' + file + ' partition_3:')
    transcript_preprocessed = preprocess(transcript_raw_3)
    res_3 = calculating_scores(transcript_preprocessed)
    del transcript_preprocessed, transcript_raw_3
    gc.collect()

    print('File ' + file + ' partition_4:')
    transcript_preprocessed = preprocess(transcript_raw_4)
    res_4 = calculating_scores(transcript_preprocessed)
    del transcript_preprocessed, transcript_raw_4
    gc.collect()

    res = pd.concat([res_1, res_2, res_3, res_4])
    res.to_csv('./transcripts/scores/ciqscores_' + file + '.csv')
    print('Result of file ' + file + ' has been saved.')
