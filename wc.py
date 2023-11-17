from pyspark import SparkContext, SparkConf

# Create a SparkConf and SparkContext
conf = SparkConf().setAppName("WordCount")
sc = SparkContext(conf=conf)

# Load a text file from your local file system
text_file = sc.textFile("file:///home/aditya/cloud/largest_file.txt")

# Split each line into words using whitespace as the delimiter
words = text_file.flatMap(lambda line: line.split())

# Map each word to a key-value pair (word, 1)
word_pairs = words.map(lambda word: (word, 1))

# Reduce by key, summing up the counts for each word
word_counts = word_pairs.reduceByKey(lambda a, b: a + b)

# Collect the top 10 words and their counts
top_words = word_counts.takeOrdered(10, key=lambda x: -x[1])

# Print the top 10 words to the terminal
for word, count in top_words:
    print(f"{word}: {count}")

# Stop the SparkContext
sc.stop()

