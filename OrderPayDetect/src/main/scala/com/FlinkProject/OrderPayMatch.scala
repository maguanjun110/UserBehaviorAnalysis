package com.FlinkProject

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.{CoProcessFunction, KeyedCoProcessFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

//对账
//订单样例类,orderevent
//payevent
case class ReceiptEvent(txId: String, paychannel: String, timestamp: Long)

object OrderPayMatch {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //从订单文件中读取数据,并转换为样例类
    val orderStream: DataStream[OrderEvent] = env.readTextFile("D:\\IntelliJ IDEA 2018.2.4\\UserBehaviorAnalysis\\OrderPayDetect\\src\\main\\resources\\OrderLog.csv")
      .map(data => {
        val str = data.split(",")
        OrderEvent(str(0).toLong, str(1), str(2), str(3).toLong)
      }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(3)) {
      override def extractTimestamp(element: OrderEvent): Long = {
        element.timestamp * 1000L
      }
    }).filter(_.txId != "") //过滤出支付id不为空的数据
      .keyBy(_.txId)

    //从支付日志中读取数据,并转换为样例类
    val receiptStream: DataStream[ReceiptEvent] = env.readTextFile("D:\\IntelliJ IDEA 2018.2.4\\UserBehaviorAnalysis\\OrderPayDetect\\src\\main\\resources\\ReceiptLog.csv")
      .map(data => {
        val str = data.split(",")
        ReceiptEvent(str(0), str(1), str(2).toLong)
      }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ReceiptEvent](Time.seconds(3)) {
      override def extractTimestamp(element: ReceiptEvent): Long = {
        element.timestamp * 1000L
      }
    }).keyBy(_.txId)

    //对两条流合并,用connect连接两条流,匹配事件进行处理
    val resultStream: DataStream[(OrderEvent, ReceiptEvent)] = orderStream.connect(receiptStream)
      .process(new OrderPayTxDetect())
    //将匹配不上的数据,在测输出流中输出
    val unmatchedPays = new OutputTag[OrderEvent]("unmatched-pay")
    val unmatchedReceipts = new OutputTag[ReceiptEvent]("unmatched-receipt")

    resultStream.print("matched")
    resultStream.getSideOutput(unmatchedPays).print("unmatched pays")
    resultStream.getSideOutput(unmatchedReceipts).print("unmatched receipts")

    env.execute("order pay tx match job")
  }
}

//自定义CoProcessFunction,实现两条流数据的匹配检验
class OrderPayTxDetect() extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {
  //用两个valuestate保存当前交易对应的支付事件和到账事件
  val payedState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("paystate", classOf[OrderEvent]))
  val receiptState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("paystate", classOf[ReceiptEvent]))
  val unmatchedPays = new OutputTag[OrderEvent]("unmatched-pay")
  val unmatchedReceipts = new OutputTag[ReceiptEvent]("unmatched-receipt")

  override def processElement1(value: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    //pay来了,考察有没有对应receipt来过
    val receipt = receiptState.value()
    if (receipt != null) {
      //如果已经有receipt,那么正常匹配,输出到主流
      out.collect((value, receipt))
      receiptState.clear()
    } else {
      //如果receipt还没来,那么把pay存入状态,注册一个定时器等待5秒?
      payedState.update(value)
      ctx.timerService().registerEventTimeTimer(value.timestamp * 1000L + 5000L)
    }

  }

  override def processElement2(value: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    //receipt来了,考察有没有对应pay来过
    val pay = payedState.value()
    if (pay != null) {
      //如果已经有pay,那么正常匹配,输出到主流
      out.collect((pay, value))
      payedState.clear()
    } else {
      //如pay还没来,那么把receipt存入状态,注册一个定时器等待3秒
      receiptState.update(value)
      ctx.timerService().registerEventTimeTimer(value.timestamp * 1000L + 3000L)
    }
  }

  override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    //定时器触发,有两种情况,所以要判断当前有没有对应的pay和receipt
    //如果pay不为空,说明receipt没来,输出unmatched pays
    if (payedState.value() != null) {
      ctx.output(unmatchedPays, payedState.value())
    }
    if (receiptState.value() != null) {
      ctx.output(unmatchedReceipts, receiptState.value())
    }
    //清空状态
    payedState.clear()
    receiptState.clear()
  }
}
