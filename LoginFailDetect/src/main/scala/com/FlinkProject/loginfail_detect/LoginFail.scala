package com.FlinkProject.loginfail_detect

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

//定义输入输出的样例类
case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

case class LoginWarning(userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String)

object LoginFail {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //从文件读取数据,map成样例类,并分配时间戳和watermark
    val loginEventStream: DataStream[LoginEvent] = env.readTextFile("D:\\IntelliJ IDEA 2018.2.4\\UserBehaviorAnalysis\\LoginFailDetect\\src\\main\\resources\\LoginLog.csv")
      .map(
        data => {
          val str = data.split(",")
          LoginEvent(str(0).toLong, str(1), str(2), str(3).toLong)
        }
      ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
      override def extractTimestamp(element: LoginEvent): Long = {
        element.eventTime * 1000L
      }
    })
    //用processFunction进行转换,如果遇到2秒内连续2次登陆失败,就输出报警
    val loginWarningStream: DataStream[LoginWarning] = loginEventStream
      .keyBy(_.userId) //按照userId分组
      .process(new LoginEventFail(2))

    loginWarningStream.print()
    env.execute("login fail job")
  }
}

//实现自定义的processFucntion 2秒内连续2次登陆
class LoginEventFail(maxFailTimes: Long) extends KeyedProcessFunction[Long, LoginEvent, LoginWarning] {
  //登陆时间状态
  lazy val loginTimeValueState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("loginTime-ValueState", classOf[Long]))
  //登陆事件状态
  lazy val loginListState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginEvent-ListState", classOf[LoginEvent]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, LoginWarning]#Context, out: Collector[LoginWarning]): Unit = {
    //拿到当前的登陆时间和登录次数

    val curLoginTime = loginTimeValueState.value()
    //如果第一次登录,注册定时器
    if (value.eventType == "fail") {
      loginListState.add(value)
      if (curLoginTime == 0) {
        ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L + 2 * 1000L)
        loginTimeValueState.update(value.eventTime * 1000L + 2 * 1000L)
      }


    } else {
      //如果登陆成功,删除定时器
      ctx.timerService().deleteEventTimeTimer(loginTimeValueState.value())
      loginListState.clear()
      loginTimeValueState.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, LoginWarning]#OnTimerContext, out: Collector[LoginWarning]): Unit = {

    val allLoginFailList: ListBuffer[LoginEvent] = new ListBuffer[LoginEvent]
    val itr = loginListState.get().iterator()
    while (itr.hasNext) {
      allLoginFailList += itr.next()
    }
    if (allLoginFailList.length >= maxFailTimes) {
      out.collect(LoginWarning(ctx.getCurrentKey, allLoginFailList.head.eventTime, allLoginFailList.last.eventTime, "login Fail in 2" +
        "s for " + allLoginFailList.length + " times."))
    }

    //清理状态
    loginListState.clear()
    loginTimeValueState.clear()
  }
}
