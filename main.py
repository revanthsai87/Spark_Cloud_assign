from pyspark.sql import SparkSession
import findspark
import os
import build_index
import rank_positive_words
import search_phrase
import search_word

findspark.init("/Users/revanth/spark-3.3.0-bin-hadoop3")
spark = SparkSession.builder.getOrCreate()
data = spark.sparkContext.wholeTextFiles("/Users/revanth/Downloads/temp")
d1 = data.map(lambda x: (os.path.basename(x[0]), x[1]))
# TASK 1
d3 = build_index.build_ind(d1)
# # TASK 2
search_word.search(d3,"atari")
# # TASK 3
search_phrase.and_phrase(d3,"technical page systems")
search_phrase.or_phrase(d3,"use speed dos dight")
# # TASK 4
rank_positive_words.rank_words(spark,d3)

spark.stop()
