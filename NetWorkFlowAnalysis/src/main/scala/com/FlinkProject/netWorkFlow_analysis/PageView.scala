
import org.apache.flink.api.common.functions.{AggregateFunction, MapFunction}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

//pv

//定义输入 输出的样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

case class PvCountView(windowEnd: Long, count: Long)

object PageView {
  def main(args: Array[String]): Unit = {
    //环境创建
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置时间语义
    env.setParallelism(4)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //确定数据源,并 定义watermark和提取时间戳
    val inputStream: DataStream[String] = env.readTextFile("D:\\IntelliJ IDEA 2018.2.4\\UserBehaviorAnalysis\\NetWorkFlowAnalysis\\src\\main\\resources\\UserBehavior.csv")
    //数据源封装为输入样例类UserBehavior
    val dataStream: DataStream[UserBehavior] = inputStream.map(
      line => {
        val str = line.split(",")
        UserBehavior(str(0).trim.toLong, str(1).trim.toLong, str(2).trim.toInt, str(3).trim, str(4).trim.toLong)
      }
    ).assignAscendingTimestamps(_.timestamp * 1000L) //升序数据,延迟数据为0

    //分配key,包装为二元组开窗聚合
    val pvStream: DataStream[PvCountView] = dataStream
      .filter(_.behavior == "pv")
      .map(new myMapper)
      /* .map(data => { //此处将所有数据放入一个任务,数据倾斜问题
         ("pv", 1L)
       })*/
      //map成二元组("pv",cunt)
      .keyBy(_._1) //把所有数据分到一组总统计
      .timeWindow(Time.hours(1)) //开一小时的滚动窗口统计
      .aggregate(new PvCountAgg, new PVCuntResult) //为获取到window信息
    //把各分区的结果汇总起来
    val resultStream: DataStream[PvCountView] = pvStream
      .keyBy(_.windowEnd)
      //.sum("count") //使用sum会将上游不同分区数据分批迭代输出,还要判断找出最新值
      .process(new TotalPvCountResult())

    resultStream.print("pv Count")

    env.execute("Tumbling window pv count job")

  }
}

class PvCountAgg() extends AggregateFunction[(String, Long), Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: (String, Long), acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class PVCuntResult() extends WindowFunction[Long, PvCountView, String, TimeWindow] {
  //此处key为dumb key 哑(无用的)
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PvCountView]): Unit = {
    val windowEnd = window.getEnd
    val count = input.iterator.next()
    out.collect(PvCountView(windowEnd, count))
  }
}

//自定义mapFunction,随机生成key,将key值均匀分配
class myMapper extends MapFunction[UserBehavior, (String, Long)] {
  override def map(value: UserBehavior): (String, Long) = {

    //key值大小影响运算速度:如何确定key,不让key太分散,以防止下游数据倾斜
    (Random.nextString(10), 1L)
  }
}

//将相同窗口内的不同的10个key放入状态中,做聚合操作
class TotalPvCountResult() extends KeyedProcessFunction[Long, PvCountView, PvCountView] {
  lazy val valueState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("totalCount", classOf[Long]))

  override def processElement(value: PvCountView, ctx: KeyedProcessFunction[Long, PvCountView, PvCountView]#Context, out: Collector[PvCountView]): Unit = {
    val currenttotalCount = valueState.value()
    //加上新的count值,更新状态
    valueState.update(currenttotalCount + value.count)
    //注册定时器 windowEnd+1ms触发
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  //注意,keyBy(随机key值后),上游10个随机生成的key值会进入10个分区,每个分区内做局部聚合操作(调window后的aggregate函数),
  // 上游10个分区的相同窗口内的数据并行进入下游,会根据最小木桶原理取最小的watermark作为下游的watermark,
  // 当最后一个watermark=windowEnd时,说明上游所有属于该窗口的数据均已到齐;keyBy(windowEnd)数据均已到达,此时触发定时器,运算输出即可
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PvCountView, PvCountView]#OnTimerContext, out: Collector[PvCountView]): Unit = {
    //定时器触发的时候,所有有分区count值均已到达输出totalcount
    out.collect(PvCountView(ctx.getCurrentKey, valueState.value()))
    valueState.clear()
  }
}

