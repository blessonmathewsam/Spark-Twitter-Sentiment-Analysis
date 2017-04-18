import java.util

import SentimentAnalyzer.mainSentiment
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Durations, StreamingContext}
import twitter4j.Status

import scala.io.Source
import scala.util.parsing.json.JSONObject

/**
  * Created by blessonm on 3/27/2017.
  */
object TwitterStreaming {

  def setupTwitter() = {
    for (line <- Source.fromInputStream(getClass.getResourceAsStream("/twitter.txt")).getLines) {
      val fields = line.split(" ")
      if (fields.length == 2) {
        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
      }
    }
  }

  def getKafkaProducer(kafkaBrokers: String): KafkaProducer[String, Object] = {
    val props = new util.HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    new KafkaProducer[String, Object](props)
  }

  def checkGeoLocation(tweet: Status): Boolean = {
    val check = Option(tweet.getGeoLocation) match {
      case Some(_) => true
      case None => false
    }
    check
  }

  def tweetToMap(tweet: Status, stopWordsList: Broadcast[List[String]]): Map[String, Object] = {
    val user = tweet.getUser
    val locationMap = Map[String, Object](
          "lat" -> tweet.getGeoLocation.getLatitude.toString,
          "lon" -> tweet.getGeoLocation.getLongitude.toString
        )
    val userMap = Map[String, Object](
      "id" -> user.getId.toString,
      "name" -> user.getName,
      "screen_name" -> user.getScreenName,
      "profile_image_url" -> user.getProfileImageURL
    )

    val text: String = tweet.getText.toLowerCase().replaceAll("\n", "")
                        .replaceAll("rt\\s+", "")
                        .replaceAll("\\s+@\\w+", "")
                        .replaceAll("@\\w+", "")
                        .replaceAll("\\s+#\\w+", "")
                        .replaceAll("#\\w+", "")
                        .replaceAll("(?:https?|http?)://[\\w/%.-]+", "")
                        .replaceAll("(?:https?|http?)://[\\w/%.-]+\\s+", "")
                        .replaceAll("(?:https?|http?)//[\\w/%.-]+\\s+", "")
                        .replaceAll("(?:https?|http?)//[\\w/%.-]+", "")
                        .split("\\W+")
                        .filter(_.matches("^[a-zA-Z]+$"))
                        .filter(!stopWordsList.value.contains(_))
                        .mkString(" ")

    return Map[String, Object](
      "user" -> JSONObject(userMap),
      "location" -> JSONObject(locationMap),
      "id" -> tweet.getId.toString,
      "created_at" -> tweet.getCreatedAt.toString,
      "text" -> tweet.getText,
      "sentiment" -> mainSentiment(text).toString
    )
  }

  def main(args: Array[String]): Unit = {

    setupTwitter()

    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .set("spark.serializer", classOf[KryoSerializer].getCanonicalName)
      .set("spark.eventLog.enabled", "true")
      .set("spark.streaming.unpersist", "true")

    val ssc = new StreamingContext(conf, Durations.seconds(10))

    val stopWords = Source.fromInputStream(getClass.getResourceAsStream("/stopwords.txt")).getLines.toList

    val stopWordsList = ssc.sparkContext.broadcast(stopWords)

    val tweetStream = TwitterUtils.createStream(ssc, None)
    val engTweets = tweetStream.filter(status => status.getLang == "en")

    engTweets.foreachRDD(tweetRDD => {
      if (tweetRDD != null && !tweetRDD.isEmpty() && !tweetRDD.partitions.isEmpty) {
        tweetRDD.foreachPartition(tweetPartition => {
          val producer = getKafkaProducer("kafka:9092")
          val filteredTweets = tweetPartition.filter(tweet => checkGeoLocation(tweet))
          filteredTweets.foreach(tweet => {
            val tweetMap = JSONObject(tweetToMap(tweet, stopWordsList))
            println(tweetMap)
            val kafkaRecord = new ProducerRecord[String, Object]("TwitterStream", null, null, tweetMap.toString())
            producer.send(kafkaRecord)
          })
          producer.close()
        })
      }
    })

    ssc.checkpoint("/tmp/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }

}
