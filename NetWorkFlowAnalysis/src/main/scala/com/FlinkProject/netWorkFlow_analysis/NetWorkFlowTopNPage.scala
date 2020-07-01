package com.FlinkProject.netWorkFlow_analysis

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

//定义输入数据样例类
case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

//聚合结果样例类
case class PageViewCount(url: String, WindowEnd: Long, count: Long)

object NetWorkFlowTopNPage {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //从文件读取数据
    //val inputStream = env.readTextFile("D:\\IntelliJ IDEA 2018.2.4\\UserBehaviorAnalysis\\NetWorkFlowAnalysis\\src\\main\\resources\\apache.log")
    //从socket中读取数据
    val inputStream = env.socketTextStream("hadoop101", 9999)
    //定义侧输出流标签
    val outputTag: OutputTag[ApacheLogEvent] = new OutputTag[ApacheLogEvent]("sideOutputStream")
    //封装样例类类型
    val dataStream: DataStream[ApacheLogEvent] = inputStream.map(line => {
      val str = line.split(" ")
      //将时间字段转换成时间戳
      val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      val timestamp = simpleDateFormat.parse(str(3)).getTime

      ApacheLogEvent(str(0), str(1), timestamp, str(5), str(6))
    })
      //设置时间戳和水位线
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
      override def extractTimestamp(element: ApacheLogEvent): Long = {
        element.eventTime
      }
    })
    //分组,开窗,聚合
    //需要注意,watermark虽然可以处理乱序数据,但也会造成数据的时效性延迟问题,所以watermark不应设置太大(乱序程度:窗口结束时间-最晚迟到数据时间),但还需要处理乱序数据,
    // Flink提供了三种方式
    // :1.watermark,
    // 2.allowedLateness()允许超出水位线,但还在allowed范围内的延迟数据到来后继续触发窗口,以处理乱序数据
    // 3.sideOutputLateData 侧输出流的方式,将极端延迟数据收集到测输出流,以做处理;
    val aggregateStream: DataStream[PageViewCount] = dataStream.keyBy(_.url)
      //assign窗口
      .timeWindow(Time.minutes(10), Time.seconds(5))
      //设置允许延迟时间
      .allowedLateness(Time.seconds(60))
      //设置测输出流
      .sideOutputLateData(outputTag)
      .aggregate(new URLPVAggFunction, new URLPVWindowFunction)


    //基于上述每个窗口的统计值排序输出
    val result: DataStream[String] = aggregateStream.keyBy(_.WindowEnd)
      .process(new SortProcessFunction(3))
    //侧输出流
    val sideOutputStream: DataStream[ApacheLogEvent] = result.getSideOutput(outputTag)

    dataStream.print("data")
    aggregateStream.print("agg result")
    //打印主流数据
    result.print("PV result")
    //打印侧流数据
    //sideOutputStream.print("side output Stream")

    env.execute("pagView topN job")
  }
}

class URLPVAggFunction extends AggregateFunction[ApacheLogEvent, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: ApacheLogEvent, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class URLPVWindowFunction extends WindowFunction[Long, PageViewCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PageViewCount]): Unit = {
    val url = key
    val windowEnd = window.getEnd
    val count = input.iterator.next()
    out.collect(PageViewCount(url, windowEnd, count))
  }
}

/*
测试数据:
1)83.149.9.216 - - 17/05/2015:10:05:49 +0000 GET /presentations/
2)83.149.9.216 - - 17/05/2015:10:05:50 +0000 GET /presentations/
3)83.149.9.216 - - 17/05/2015:10:05:51 +0000 GET /presentations/
4)83.149.9.216 - - 17/05/2015:10:05:52 +0000 GET /presentations/
5)83.149.9.216 - - 17/05/2015:10:05:46 +0000 GET /presentations/
6)83.149.9.216 - - 17/05/2015:10:05:53 +0000 GET /presentations/

 */
//bug1:通过测试发现,当输入第一条数据,会输出一条主流数据 data> ApacheLogEvent(83.149.9.216,-,1431829554000,GET,/presentations/)
//当输入第二条数据,会再输出一条主流数据, data> ApacheLogEvent(83.149.9.216,-,1431829555000,GET,/presentations/),但不会触发窗口操作,因为watermark=最大事件时间-延迟时间1秒< windowend 的时间
//当输入第三条数据,会再输出一条主流数据,data> ApacheLogEvent(83.149.9.216,-,1431829556000,GET,/presentations/),触发窗口操作[10:05:45,10:05:50),因为watermark=最大事件时间10:05:51-延迟时间1秒 >= windowEnd(10:05:50) agg result > PageViewCount(/presentations/,10:05:50,1) 因为属于10:05:50窗口的数据只有一条即49来的数据,50秒来的数据不属于窗口,说明窗口包头不包尾
//但是不会触发下一阶段任务的定时器,因为定时器的触发时间是:windowEnd+1ms,只有watermark>windowEnd10:05:50+1ms,才会触发定时器,输出最终结果 PV result>时间:2015-05-17 10:05:50 top1: 页面url=/presentations/ 访问量=1
//当输入第四条数据,会再输出一条主流数据,data> ApacheLogEvent(83.149.9.216,-,1431829557000,GET,/presentations/),第二阶段任务会触发,因为此时watermark=10:05:52-1 > 定时器的触发时间:windowEnd10:05:50+1ms;第一阶段数据会进入第二阶段的List状态,输出最终结果
//当输入第五条数据,除会输出一条主流数据,data> ApacheLogEvent(83.149.9.216,-,1431829551000,GET,/presentations/),还会触发一次窗口[10:05:45,10:05:50),这是因为我们设置了allowedLateness(60s),windowEnd10:05:50 至 windowEnd10:05:50+60ms范围内来的属于[10:05:45,10:05:50)窗口的数据还会触发一次 agg result >PageViewCount(/presentations/,10:05:50,2),
// 但是下游任务不会触发定时器,虽然上游每来一条数据,下游注册一个定时器,且因为已经输出国一次,watermark变为10:05:51,但新来的迟到数据还是不能触发定时器,需要再来一条watermark才会触发定时器
//即输入第六条数据的时候,会输出结果 result>PV result>时间:2015-05-17 10:05:50 top1: 页面url=/presentations/ 访问量=1;而不是我们预期的result>PV result>时间:2015-05-17 10:05:50 top1: 页面url=/presentations/ 访问量=2;为什么???
//这是因为我们在第二阶段任务中,每触发一次定时器,会将listState状态进行清空,n那么导致后面来的延迟数据重新排序,不能再原来结果上排序.怎么解决????
//可以在第二阶段再定义一个定时器,只有当allowedlateness时间戳内来的延迟数据处理完,才会触发新的定时器,清空ListState
//上述的策略虽然可以解决历史数据的丢失问题,但是会出现新的问题:即修改后的代码,执行完第六条数据,会输出PV result>时间:2015-05-17 10:05:50 top1: 页面url=/presentations/ 访问量=2;top2: 页面url=/presentations/ 访问量=1;相同数据重复输出,为什么???
//这是因为当输入至第四条数据,第一阶段 触发窗口[10:05:45,10:05:50)(其实第三条即触发窗口),agg result >PageViewCount(/presentations/,10:05:50,1),第二阶段,基于第一阶段结果,触发定时器,将状态保存到listState中,最终输出:输出结果为 PV result>时间:2015-05-17 10:05:50  页面url=/presentations/ 访问量=1
//当输入至第五条数据,第一阶段窗口[10:05:45,10:05:50)又被触发一次,输出结果:agg result >PageViewCount(/presentations/,10:05:50,2),输入至第六条数据,第二阶段触发定时器,将状态保存到listState中,最终输出结果为:PV result>时间:2015-05-17 10:05:50 top1: 页面url=/presentations/ 访问量=2;top2: 页面url=/presentations/ 访问量=1;
//解决办法:将listState换位MapState,相同的url数据要替换,而不是listState的追加


class SortProcessFunction(num: Int) extends KeyedProcessFunction[Long, PageViewCount, String] {
  //此处将listState替换为mapState,相同url的数据,会被放入两个,造成刷屏
 // lazy val listState: ListState[PageViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[PageViewCount]("PVListState", classOf[PageViewCount]))
  lazy val mapState:MapState[String,Long]= getRuntimeContext.getMapState(new MapStateDescriptor[String,Long]("mapState-logevent",classOf[String],classOf[Long]))

  override def processElement(value: PageViewCount, ctx: KeyedProcessFunction[Long, PageViewCount, String]#Context, out: Collector[String]): Unit = {
    //listState.add(value)
    mapState.put(value.url,value.count)
    //定义定时器
    ctx.timerService().registerEventTimeTimer(value.WindowEnd + 100L)
    ctx.timerService().registerEventTimeTimer(value.WindowEnd + 60 * 1000L)
  }

  //此处的timestamp是定时器的时间:定时器的timestamp取决于事件数据的事件时间,当事件时间大于或等于定时器时间,触发定时器,同时将定时器时间timestamp赋值为触发的是定时器的时间
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

    if (timestamp == ctx.getCurrentKey + 60 * 1000L) {
      //清除状态
      mapState.clear()
    }
    val list: ListBuffer[(String,Long)] = ListBuffer()
    val itr= mapState.entries().iterator()
    while(itr.hasNext){
      val entry = itr.next()
      list += ((entry.getKey,entry.getValue))
    }

    //排序
    val topNList: ListBuffer[(String,Long)] = list.sortWith(_._2 > _._2).take(num)


    //创建一个StringBuilder,将同一窗口中的PageViewEvent拼接后输出
    val pvString = new StringBuilder
    pvString.append("时间:").append(new Timestamp(timestamp - 100)).append("\n")
    //遍历topN列表,打印输出
    for (i <- topNList.indices) {
      val pageview = topNList(i)
      pvString.append("Top").append(i + 1).append(":")
        .append(" 页面url=").append(pageview._1)
        .append(" 页面访问量count=").append(pageview._2).append("\n")
    }
    pvString.append("=======================================")

    //输出结果
    Thread.sleep(1000)
    out.collect(pvString.toString())

  }
}
/*
[root@hadoop101 ~]# nc -lk 9999
83.149.9.216 - - 17/05/2015:10:05:49 +0000 GET /presentations/
83.149.9.216 - - 17/05/2015:10:05:50 +0000 GET /presentations/
83.149.9.216 - - 17/05/2015:10:05:51 +0000 GET /presentations/
83.149.9.216 - - 17/05/2015:10:05:52 +0000 GET /presentations/
83.149.9.216 - - 17/05/2015:10:05:46 +0000 GET /presentations/
83.149.9.216 - - 17/05/2015:10:05:53 +0000 GET /presentations/
83.149.9.216 - - 17/05/2015:10:05:46 +0000 GET /present
83.149.9.216 - - 17/05/2015:10:05:54 +0000 GET /present

data> ApacheLogEvent(83.149.9.216,-,1431828349000,GET,/presentations/)
data> ApacheLogEvent(83.149.9.216,-,1431828350000,GET,/presentations/)
data> ApacheLogEvent(83.149.9.216,-,1431828351000,GET,/presentations/)
agg result> PageViewCount(/presentations/,1431828350000,1)
data> ApacheLogEvent(83.149.9.216,-,1431828352000,GET,/presentations/)
PV result> 时间:2015-05-17 10:05:50.0
Top1: 页面url=/presentations/ 页面访问量count=1
=======================================
data> ApacheLogEvent(83.149.9.216,-,1431828346000,GET,/presentations/)
agg result> PageViewCount(/presentations/,1431828350000,2)
data> ApacheLogEvent(83.149.9.216,-,1431828353000,GET,/presentations/)
PV result> 时间:2015-05-17 10:05:50.0
Top1: 页面url=/presentations/ 页面访问量count=2
=======================================
data> ApacheLogEvent(83.149.9.216,-,1431828346000,GET,/present)
agg result> PageViewCount(/present,1431828350000,1)
data> ApacheLogEvent(83.149.9.216,-,1431828354000,GET,/present)
PV result> 时间:2015-05-17 10:05:50.0
Top1: 页面url=/presentations/ 页面访问量count=2
Top2: 页面url=/present 页面访问量count=1
=======================================
 */