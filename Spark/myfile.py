from pyspark import SparkContext
import sys



# create Spark context with necessary configuration
sc = SparkContext(appName = "myfile")

# read data from text file and split each line into words
words =sc.textFile("Books/Ulysses.txt").flatMap(lambda line: line.split(" "))

# count the occurrence of each word
wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b:a + b)
# save the counts to output

wordCounts.saveAsTextFile("file:///home/consultant/Desktop/Feuerzeug/output/")


