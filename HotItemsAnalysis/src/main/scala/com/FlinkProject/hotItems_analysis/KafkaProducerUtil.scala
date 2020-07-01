package com.FlinkProject.hotItems_analysis

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object KafkaProducerUtil {
  def main(args: Array[String]): Unit = {
    writeToKafkaWithTopic("hotItems")
  }
  def writeToKafkaWithTopic(topic:String): Unit ={
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","hadoop102:9092,hadoop103:9092,hadoop101:9092")
    properties.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer")

    //创建一个kafkaProducer,用它来发送数据
    val producer = new KafkaProducer[String,String](properties)

    //从文件中读取数据,逐条发送
    val bufferedSource = io.Source.fromFile("D:\\IntelliJ IDEA 2018.2.4\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
    for(line <- bufferedSource){
      val record = new ProducerRecord[String,String](topic,line.toString)
      producer.send(record)
    }
    producer.close()
  }
}
