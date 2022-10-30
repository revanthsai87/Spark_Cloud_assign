import re
import Textclean as tc
import time
from nltk import PorterStemmer
from nltk.corpus import stopwords

import savefile


def build_ind(d1):
    s_time = time.time()
    stem = PorterStemmer()
    stop_words = set(stopwords.words('english'))
    d3 = d1.flatMap(lambda x: tc.dw(x[0], x[1])).map(lambda p: ([p[0][0], p[1]], p[0][1])).flatMapValues(lambda x: tc.modify(x)).map(lambda x: (x[1][0], [x[0][0], x[0][1], x[1][1]])).groupByKey().map(lambda x: (x[0], list(x[1])))
    d3 = d3.filter(lambda x: x[0] != '')
    d3 = d3.filter(lambda x: bool(re.search("\d+", x[0])) == False)
    d3 = d3.filter(lambda x: bool(re.search("[^A-Za-z0-9]+", x[0])) == False)
    d3 = d3.filter(lambda x: len(x[0]) > 1)
    d3 = d3.filter(lambda x: x[0] not in stopwords.words('english'))
    # print(d3.take(10))
    savefile.save(d3,"inverted_index")
    print("--- %s seconds ---" % (time.time() - s_time))
    return d3
