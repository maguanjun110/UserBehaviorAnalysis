package com.FlinkProject.hotItems_analysis

import java.sql.Timestamp


import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/*
定义输入数据的样例类
 */
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

//定义窗口聚合结果的样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)
/**
  * 需求:每5分钟统计1小时内,热门商品PV前十名
 */
object HotItems {
  def main(args: Array[String]): Unit = {
    //使用datastream API
    //1.创建流处理执行环境
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //2.环境的配置
    //2.1并行度设为1
    streamEnv.setParallelism(1)
    //2.2 设置时间语义 事件时间 历史数据的回放
    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //3.读取数据源
    val inputStream: DataStream[String] = streamEnv.readTextFile("D:\\IntelliJ IDEA 2018.2.4\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")

    //从kafka读取数据  单值传输
    /*
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","hadoop102:9092,hadoop103:9092,hadoop101:9092")
    properties.setProperty("group.id","consumer-group")
    properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset","latest")
    val inputStream: DataStream[String] = streamEnv.addSource(new FlinkKafkaConsumer[String]("hotItems",new SimpleStringSchema(),properties ))
   */
    //4.转换为样例类类型的数据
    val dataStream: DataStream[UserBehavior] = inputStream.map(line => {
      val str = line.split(",")
      UserBehavior(str(0).trim.toLong, str(1).trim.toLong, str(2).trim.toInt, str(3).trim, str(4).trim.toLong)
    })
      //5.设置时间戳及watermark---当前数据没有延迟,所以不用设置watermark
      //dataStream.assignTimestampsAndWatermarks()
      .assignAscendingTimestamps(_.timestamp * 1000L)

      //6.对数据进行转换 逻辑操作
      //6.1过滤数"pv"行为
      .filter("pv" == _.behavior)
    //6.2分组,开窗聚合统计个数
    val aggDataStream: DataStream[ItemViewCount] = dataStream.keyBy(_.itemId)
      .timeWindow(Time.hours(1), Time.minutes(5))
      //aggregate 作用于每一个key 参数一:定义窗口聚合规则 参数二:定义输出数据结构 结合增量聚合函数及拿到窗口信息
      .aggregate(new PVAggregateFunction, new ItemCountWindowFunction)
    //7.组内排序,取topN
    val resultDataStream: DataStream[String] = aggDataStream.keyBy(_.windowEnd)
      .process(new SortProcessFunction(5))

    resultDataStream.print("result ")
    //执行任务
    streamEnv.execute("Hot Items job")
  }


}

//自定义聚合函数,作用于每一key,每来一条数据,加一
class PVAggregateFunction extends AggregateFunction[UserBehavior, Long, Long] {
  //初始值
  override def createAccumulator(): Long = 0

  //来一条数据就加一
  override def add(in: UserBehavior, acc: Long): Long = acc + 1

  //当前acc的值就是返回值
  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

//自定义窗口函数,结合window信息包装成 IN:预聚合函数的输出类型,OUT:输出数据类型,即封装的样例类ItemViewCount, KEY:String(如果使用keyBy("itemId"),KEY为Tuple;如果使用keyBy(_.itemId),KEY为本来数据类型,即String)
class ItemCountWindowFunction extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val itemId: Long = key
    val windowEnd: Long = window.getEnd
    val count: Long = input.iterator.next()
    out.collect(ItemViewCount(itemId, windowEnd, count))
  }
}

//自定义keyedProcessFunction

class SortProcessFunction(num: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {
  //定义list状态,用于保存统一窗口内的数据,用于topN
  lazy val listState: ListState[ItemViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("ItemViewCountListState", classOf[ItemViewCount]))

  override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Long, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
    //每来一条数据,就把它保存到状态中
    listState.add(i)
    //注册定时器,在windowEnd +100毫秒后触发;keyBy之后,定义的定时器只作用域一个key
    context.timerService().registerEventTimeTimer(i.windowEnd + 100)
  }

  //定时器触发时,从状态中取数据,然后排序输出:此处的timestamp是指定时器的触发时间
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //先把状态中的数据提取到一个ListBuffer中
    val allIntemCountList: ListBuffer[ItemViewCount] = ListBuffer()
    import scala.collection.JavaConversions._
    for (itemCount <- listState.get()) {
      allIntemCountList += itemCount
    }
    //按照count值大小排序,取topN
    val sortedItemCountList = allIntemCountList.sortWith(_.count>_.count).take(num)
    //val sortedItemCountList = allIntemCountList.sortBy(_.count)(Ordering.Long.reverse).take(num)
    //清除状态的操作
    listState.clear()

    //将排名信息格式化成字符串,方便监控显示
    val result:StringBuilder = new StringBuilder
    result.append("时间: ").append(new Timestamp(timestamp-100)).append("\n")
    //遍历sorted列表,输出TopN信息
    for(i <- sortedItemCountList.indices){
      val currentItemCount = sortedItemCountList(i)
      result.append("Top").append(i+1).append(":")
        .append(" 商品ID=").append(currentItemCount.itemId)
        .append(" 访问量=").append(currentItemCount.count)
        .append("\n")
    }
    result.append("=============================\n\n")
    //控制输出频率
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}

