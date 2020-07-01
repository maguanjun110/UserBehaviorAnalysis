package com.FlinkProject

import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

//定义输入输出的样例类
case class OrderEvent(orderId: Long, eventType: String, txId: String, timestamp: Long)

case class OrderStatus(orderId: Long, resultMsg: String)

object OrderTimeOut {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //从文件中读取数据,并转换为样例类
    val inputStream = env.readTextFile("D:\\IntelliJ IDEA 2018.2.4\\UserBehaviorAnalysis\\OrderPayDetect\\src\\main\\resources\\OrderLog.csv")
      .map(data => {
        val str = data.split(",")
        OrderEvent(str(0).toLong, str(1), str(2), str(3).toLong)
      }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(3)) {
      override def extractTimestamp(element: OrderEvent): Long = {
        element.timestamp * 1000L
      }
    })

    //1.定义一个要匹配的事件序列的模式
    val orderPayPattern = Pattern
      .begin[OrderEvent]("create").where(_.eventType == "create") //首先是订单的create事件
      .followedBy("pay").where(_.eventType == "pay") //后面来的是订单的pay事件
      .within(Time.minutes(15))

    //2.将pattern应用在按照orderId分组的数据流上
    val patternStream = CEP.pattern(inputStream.keyBy(_.orderId), orderPayPattern)

    //3.定义一个侧输出流标签,用来将标明超时事件的侧输出流
    val orderTimeoutOutPutTag = new OutputTag[OrderStatus]("order timeout")

    //4.调用select 方法,提取匹配事件和超时时间,分别进行处理转换输出
    val resultStream: DataStream[OrderStatus] = patternStream
      .select(orderTimeoutOutPutTag, new OrderTimeOutSelect(), new OrderPaySelect())

    //5.打印输出
    resultStream.print("payed")
    resultStream.getSideOutput(orderTimeoutOutPutTag).print("timeout")

    env.execute("order timeout job")
  }
}

//自定义超时处理函数
class OrderTimeOutSelect() extends PatternTimeoutFunction[OrderEvent, OrderStatus] {
  override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderStatus = {
    val timeOutOrderId = map.get("create").get(0).orderId
    OrderStatus(timeOutOrderId, "order time out" + l)
  }
}
//自定义匹配处理函数
class OrderPaySelect() extends PatternSelectFunction[OrderEvent,OrderStatus]{
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderStatus = {
    val payedOrderId = map.get("pay").get(0).orderId
    OrderStatus(payedOrderId,"payed successfully")
  }
}
