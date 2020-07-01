package com.FlinkProject

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object OrderTimeOutWithoutCEP {
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
    val orderStatusStream: DataStream[OrderStatus] =
      inputStream
        .keyBy(_.orderId)
        //自定义processFunction,做精细化的流程控制
        .process(new OrderPayMatchDetect())

    //打印输出
    orderStatusStream.print("payed")
    orderStatusStream.getSideOutput(new OutputTag[OrderStatus]("timeout")).print("timeout")

    env.execute("order timeout without cep job")
  }
}

//实现自定义keyedstreamProcessFunction,主流输出正常支付的订单,侧输出流输出超时报警订单
class OrderPayMatchDetect() extends KeyedProcessFunction[Long, OrderEvent, OrderStatus] {
  //定义状态,用来保存是否来过create和pay事件的标识位,以及定时器时间戳
  lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isPayedState", classOf[Boolean]))
  lazy val isCreatedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isCreatedState", classOf[Boolean]))
  lazy val timerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timerTsState", classOf[Long]))
  val orderTimeoutOutputTag = new OutputTag[OrderStatus]("timeout")

  override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderStatus]#Context, out: Collector[OrderStatus]): Unit = {
    //先取出当前状态
    val isPayed = isPayedState.value()
    val isCreated = isCreatedState.value()
    val timerTs = timerTsState.value()



    //判断当前事件的类型,分成不同情况讨论
    //情况1:来的是created,要继续判断之前是够有pay来过
    if (value.eventType == "create") {
      //情况1.1:如果已经pay过的话,匹配成功,输出到主流,清空状态
      if (isPayed) {
        //该情况应该还需判断,pay和create之间的时间相差在15分钟内
        out.collect(OrderStatus(value.orderId, "payed successfully"))
        isPayedState.clear()
        timerTsState.clear()
        ctx.timerService().deleteEventTimeTimer(timerTs)
      }
      //情况1.2:如果没有pay过,n那么就注册一个15分钟后的定时器,更新状态,开始等待
      else {
        val ts = value.timestamp * 1000L + 15 * 60 * 1000L
        ctx.timerService().registerEventTimeTimer(ts)
        timerTsState.update(ts)
        isCreatedState.update(true)
      }
    }
    //情况2:来的是pay,要继续判断是否来过create
    else if (value.eventType == "pay") {
      //情况2.1:如果create已经来过,匹配成功,要继续判断间隔时间是否超过了15分钟
      if (isCreated) {
        //情况2.1.1:如果没有超时,正常输出结果到主流
        if (value.timestamp * 1000L < timerTs) {
          out.collect(OrderStatus(value.orderId, "payed successfully"))
        }
        //情况2.1.2;如果已经超时,输出timeOut报警到侧输出流
        else {
          ctx.output[OrderStatus](orderTimeoutOutputTag, OrderStatus(value.orderId, "payed but already timeout"))

        }
        //不论哪种情况,都已经有了输出,所以清空状态
        ctx.timerService().deleteEventTimeTimer(timerTs)
        isCreatedState.clear()
        timerTsState.clear()

      }
      //情况2.2:如果create没来,需要等待乱序create,注册一个当前pay时间戳的定时器
      else {
        val ts = value.timestamp * 1000L
        ctx.timerService().registerEventTimeTimer(ts)
        timerTsState.update(ts)
        isPayedState.update(true)
      }
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderStatus]#OnTimerContext, out: Collector[OrderStatus]): Unit = {
    //定时器触发,要判断是哪种情况
    if (isPayedState.value()) {
      //如果pay过,说明create没来,可能出现了数据丢失异常
      ctx.output(orderTimeoutOutputTag, OrderStatus(ctx.getCurrentKey, "already payed but not found created log "))
    } else {
      //如果没有pay过,说明真正15分钟超时报警
      ctx.output(orderTimeoutOutputTag, OrderStatus(ctx.getCurrentKey, "order timeout"))
    }
    //清理状态
    isPayedState.clear()
    isCreatedState.clear()
    timerTsState.clear()
  }
}
