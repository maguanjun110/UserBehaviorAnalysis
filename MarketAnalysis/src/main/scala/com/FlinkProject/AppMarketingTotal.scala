package com.FlinkProject

import org.apache.flink.api.common.functions.{AggregateFunction, MapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

case class TotalAppCount(windowEnd: String, count: Long)

object AppMarketingTotal {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val inputStream: DataStream[MarketUserBehavior] = env.addSource(new SimulateMarketEventSource())
      .assignAscendingTimestamps(_.timestamp)
    val aggStream: DataStream[TotalAppCount] = inputStream
      .filter(_.behavior != "uninstall")
      .map(new MyAppMarketingCountMapper())
      .keyBy(_._1)
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(new AppCountAggFunction(), new AppMarketCountWindowFunction())
    val resultStream: DataStream[(String, Long)] = aggStream
      .keyBy(_.windowEnd)
      .process(new TotalAppMarketCountProcessFunction())
    resultStream.print()
    env.execute("total App Market Count job")
  }
}


class MyAppMarketingCountMapper() extends MapFunction[MarketUserBehavior, (String, Long)] {
  override def map(value: MarketUserBehavior): (String, Long) = {
    val rand: String = Random.nextInt(8).toString
    (rand, 1L)
  }
}

class AppCountAggFunction() extends AggregateFunction[(String, Long), Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: (String, Long), accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b

}

class AppMarketCountWindowFunction() extends WindowFunction[Long, TotalAppCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[TotalAppCount]): Unit = {

    val windowEnd = window.getEnd.toString
    val count = input.iterator.next()
    out.collect(TotalAppCount(windowEnd, count))

  }
}

class TotalAppMarketCountProcessFunction() extends KeyedProcessFunction[String, TotalAppCount, (String, Long)] {
  lazy val marketValueState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("totalAppMarketCountValueState", classOf[Long]))

  override def processElement(value: TotalAppCount, ctx: KeyedProcessFunction[String, TotalAppCount, (String, Long)]#Context, out: Collector[(String, Long)]): Unit = {
    marketValueState.update(marketValueState.value() + value.count)
    val timer = ctx.timerService().registerEventTimeTimer(value.windowEnd.toLong + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, TotalAppCount, (String, Long)]#OnTimerContext, out: Collector[(String, Long)]): Unit = {
    out.collect((ctx.getCurrentKey, marketValueState.value()))
    marketValueState.clear()
  }
}


