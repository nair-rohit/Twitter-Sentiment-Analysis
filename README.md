# Twitter Sentiment Analysis
 A Spark Streaming, Kafka based Twitter Sentiment Analysis Engine

The code is for performing Sentiment Analysis by setting up the data processing pipeline as follows:

Twitter API -> Producer -> Kafka Topic -> Spark Streaming Kafka Consumer -> ElasticSearch -> Kibana (Visualization)

Dependencies for python:
tweepy
kafka
pyspark
elasticsearch
pycore-nlp

Compiled and run on 
Python 3.7
Spark - 2.11:2.2.1 (Scala/Spark version)
ElasticSearch - 7.6.2
Kafka - 2.11:2.0.0 (Kafka version)

Jars to download:
elasticsearch jar :elasticsearch-hadoop-7.6.2.jar
sparkstreaming jar: spark-streaming-kafka-0-8_2.11-2.4.5.jar

Execution guidelines :
	1. Run Zookeeper, Kafka
	2. Run ElasticSearch, Kibana
	3. Open a terminal/IDE and run twitterKafkaProducer.py
	4. Browse to the folder where twitterKafkaConsumer.py files are present and run using the spark submit command as follows:
		spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11-2.2.1 --jars elasticsearch-hadoop-7.6.2.jar
	5. Open the web browser --> Go to http://localhost:5601
	
	This would run with vaderSentiment. 
	
	Running with stanford nlp uses the following additional steps:
		
	1. Download stanfordcorenlp and unzip
	2. Run stanford corenlp by going to that installation folder and running on localhost
	3. By default the server will work at http://localhost:9000	
	

