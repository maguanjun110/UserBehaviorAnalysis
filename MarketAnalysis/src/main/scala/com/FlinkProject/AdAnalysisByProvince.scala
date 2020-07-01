package com.FlinkProject

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

//定义输入输出样例类
case class AdClickEvent(userId: Long, adId: Long, province: String, city: String, timestamp: Long)

case class AdCountByProvince(province: String, windowEnd: String, count: Long)

//定义侧输出流报警信息样例类
case class BlackListWarning(userId: String, adId: String, msg: String)

object AdAnalysisByProvince {
  def main(args: Array[String]): Unit = {
    //创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //从文件读取数据,然后转换成样例类,指定时间戳,读取数据源
    val resource = getClass.getResource("/AdClickLog.csv")

    val adLogStream: DataStream[AdClickEvent] = env.readTextFile(resource.getPath)
      .map(data => {
        val str: Array[String] = data.split(",")
        AdClickEvent(str(0).toLong, str(1).toLong, str(2), str(3), str(4).toLong)
      }).assignAscendingTimestamps(_.timestamp * 1000L)
    //定义刷单行为过滤操作
    val filterBlackListStream: DataStream[AdClickEvent] = adLogStream
      .keyBy(data => {
        (data.userId, data.adId)
      }).process(new FilterBlackList(100))

    //按照province分组
    val aggStream: DataStream[AdCountByProvince] = filterBlackListStream.keyBy(_.province)
      .timeWindow(Time.hours(1))
      .aggregate(new AggClickCount(), new AdCountResult())
    filterBlackListStream.getSideOutput(new OutputTag[BlackListWarning]("blackList")).print("blackList")
    aggStream.print()
    env.execute("Province Ad Count job")

  }
}

class AggClickCount() extends AggregateFunction[AdClickEvent, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: AdClickEvent, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class AdCountResult() extends WindowFunction[Long, AdCountByProvince, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdCountByProvince]): Unit = {
    out.collect(AdCountByProvince(key, window.getEnd.toString, input.iterator.next()))
  }
}

//实现自定义的processFunction,判断用户对广告的点击次数是否达标
class FilterBlackList(i: Int) extends KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent] {
  //定义状态,需要保存点击量Count
  lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count", classOf[Long]))
  //保存标识位,用于表示用户是否已经在黑名单中
  lazy val isSentState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-Sent", classOf[Boolean]))

  override def processElement(value: AdClickEvent, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#Context, out: Collector[AdClickEvent]): Unit = {
    //取出状态数据
    val curCount = countState.value()
    //如果是第一个数据,那么注册第二天0点的定时器,用于清空状态
    if (curCount == 0) {
      //明天零点的时间,定义定时器
      val ts = (ctx.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24) + 1) * (1000 * 60 * 60 * 24)
      ctx.timerService().registerProcessingTimeTimer(ts)
    }
    //判断count值是否达到上限,如果达到,并且之前没有输出过报警信息,那么报警
    if (curCount > i) {
      if (!isSentState.value()){
        ctx.output(new OutputTag[BlackListWarning]("blackList"),BlackListWarning(value.userId.toString,value.adId.toString,"click over "+i+" times today"))
        isSentState.update(true)
      }
      return
    }
    //count值加1
    countState.update(curCount + 1)
    out.collect(value)

  }
  //零点触发定时器,直接清空状态
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#OnTimerContext, out: Collector[AdClickEvent]): Unit =
  {
    countState.clear()
    isSentState.clear()
  }
}

