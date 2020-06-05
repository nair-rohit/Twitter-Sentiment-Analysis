
import json
from elasticsearch import Elasticsearch
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import hashlib
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from stanford-nlp-sentiment import evaluate

es = Elasticsearch()

def main():
    sentimentAnalysis()

def evaluate(text):
    sA = SentimentIntensityAnalyzer()
    polarity = sA.polarity_scores(text)
    if (polarity["compound"] > 0):
        return "positive"
    elif (polarity["compound"] < 0):
        return "nagative"
    else:
        return "neutral"


def getTrump(tweetText):
    for i in ['#trump','#donaldTrump','#donald']:
        if i in tweetText:
            return True
    return False

def getCorona(tweetText):
    for i in ['#corona','#coronavirus','#covid19','#covid-19']:
        if i in tweetText:
            return True
    return False

def createRDD(rdd):
    newRdd = {}
    newRdd['tweet'] = rdd[0]
    newRdd['sentiment'] = rdd[1]


def sentimentAnalysis():
    sc = SparkContext(appName="SparkDemo")
    ssc = StreamingContext(sc, 3)
    kstream = KafkaUtils.createStream(ssc, 'localhost:2181', 'spark-streaming', {'twitterstream':1})
    ssc.checkpoint(r'./checkpoint')
    tweetsA = kstream.map(lambda line: line[1])

    senti = tweetsA.map(lambda val: json.loads(val))
    sentiAnalysis = senti.map(lambda line: (line['text'], evaluate(line['text'])))

    trumpSenti = sentiAnalysis.filter((lambda x: getTrump(x[0].lower()))).map(lambda val: val).map(parse).map(addId)
    coronaSenti = sentiAnalysis.filter(lambda x: getCorona(x[0].lower())).map(lambda val: val).map(parse).map(addId)

    corona_es_write_conf = {
        "es.nodes": "localhost",
        "es.port": "9200",
        "es.resource": 'corona/corona-doc',
        "es.input.json": "yes",
        "es.mapping.id": "doc_id"
    }

    trump_es_write_conf = {
        "es.nodes": "localhost",
        "es.port": "9200",
        "es.resource": 'trump/trump-doc',
        "es.input.json": "yes",
        "es.mapping.id": "doc_id"
    }

    trumpSenti.foreachRDD(lambda rdd: rdd.saveAsNewAPIHadoopFile(
        path='-',
        outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
        keyClass="org.apache.hadoop.io.NullWritable",
        valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
        conf=trump_es_write_conf))

    coronaSenti.foreachRDD(lambda line : line.saveAsNewAPIHadoopFile(
            path="-",
            outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
            keyClass="org.apache.hadoop.io.NullWritable",
            valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
            conf=corona_es_write_conf))

    ssc.start()
    ssc.awaitTermination()


def addId(data):
    j=json.dumps(data).encode('ascii', 'ignore')
    data['doc_id'] = hashlib.sha224(j).hexdigest()
    return (data['doc_id'], json.dumps(data))

def parse(rdd):
    d = {}
    d['tweet']=rdd[0]
    d['senti']=rdd[1]
    return d


if __name__ == "__main__":
    main()