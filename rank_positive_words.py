import savefile


def rank_words(spark,d3):
  p_words=spark.sparkContext.textFile("/Users/revanth/Downloads/positive words.csv")
  p_words=p_words.map(lambda x:x.split(",")[1]).collect()
# positivewords="may time play applies pin communication"
# positivewords=positivewords.split(" ")
  positivewords=p_words
# d4=d3.filter(lambda x:x[0]==(positivewords[0])).flatMapValues(lambda x:x).map(lambda x:[x[1][0],1])
# for i in range(1,len(positivewords)):
#     d5 = d3.filter(lambda x: x[0] == (positivewords[i])).flatMapValues(lambda x: x).map(lambda x:[x[1][0],1])
#     d4=d5.union(d4)
  d4=d3.filter(lambda x:x[0] in positivewords)
  d5 = d4.flatMapValues(lambda x: x).map(lambda x: [x[1][0], 1])
  d5 = d5.reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1], False, 10)
  print(d5.take(20))
  d5=d5.map(lambda x:x[0])
  savefile.save(d5,"ranking_files")
  # print(d5.collect())