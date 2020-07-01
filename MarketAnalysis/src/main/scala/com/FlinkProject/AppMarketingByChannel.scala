package com.FlinkProject

import java.sql.Timestamp
import java.util.UUID

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

//定义输入数据样例类
case class MarketUserBehavior(userId: String, behavior: String, channel: String, timestamp: Long)

//定义输出统计的样例类
case class MarketViewCount(windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long)

//自定义测试数据源,可以并行生成数据的数据源
class SimulateMarketEventSource() extends RichParallelSourceFunction[MarketUserBehavior] {
  //定义是否在运行的标志位
  var running: Boolean = true
  //定义可选的推广渠道和用户行为的集合
  val behaviorSet: Seq[String] = Seq("click", "download", "install", "uninstall")
  val channelSet: Seq[String] = Seq("appstore", "huaweistore", "weiboStore", "wechat")
  //定义随机数生成器
  val rand: Random = Random

  override def run(ctx: SourceFunction.SourceContext[MarketUserBehavior]): Unit = {
    //定义一个发出数据的最大量,用于控制测试数据量
    val maxCounts = Long.MaxValue
    var count = 0L
    //while循环,不停的随机生成数据
    while (running & count < maxCounts) {
      val id:String = UUID.randomUUID().toString
      //随机生成用户行为数据
      val behavior:String = behaviorSet(rand.nextInt(behaviorSet.size))
      //随机生成channel
      val channel:String = channelSet(rand.nextInt(channelSet.size))
      val ts:Long = System.currentTimeMillis()
      //发出数据
      ctx.collect(MarketUserBehavior(id, behavior, channel, ts))
      count += 1
      Thread.sleep(50L)
    }
  }

  override def cancel(): Unit = {
    running = false
  }
}

object AppMarketingByChannel {
  def main(args: Array[String]): Unit = {
    //创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //定义输入数据源
    val inputStream: DataStream[MarketUserBehavior] = env.addSource(new SimulateMarketEventSource())
      .assignAscendingTimestamps(_.timestamp)

    val resultStream: DataStream[MarketViewCount] = inputStream
      .filter(_.behavior != "uninstall") //过滤掉卸载行为
      .keyBy(data => {
      (data.channel, data.behavior) //按照渠道和行为类型分组
    }).timeWindow(Time.hours(1), Time.seconds(5))
      .process(new MarketCountByChannel()) //自定义全窗口函数

    resultStream.print()
    env.execute("market count by channel job")

  }
}

//自定义ProcessWindowFunction
class MarketCountByChannel() extends ProcessWindowFunction[MarketUserBehavior, MarketViewCount, (String, String), TimeWindow] {
  override def process(key: (String, String), context: Context, elements: Iterable[MarketUserBehavior], out: Collector[MarketViewCount]): Unit = {
    val windowStart = new Timestamp(context.window.getStart).toString
    val windowEnd = new Timestamp(context.window.getEnd).toString
    val channle:String = key._1
    val behavior:String = key._2
    val count:Long = elements.size
    out.collect(MarketViewCount(windowStart,windowEnd,channle,behavior,count))
  }
}
