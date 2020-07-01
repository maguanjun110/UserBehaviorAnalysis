import java.util

import com.FlinkProject.loginfail_detect.{LoginEvent, LoginWarning}
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object LoginFailWithCEP {
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

    //1.定义一个匹配的模式
    val loginFailPattern: Pattern[LoginEvent, LoginEvent] = Pattern
      .begin[LoginEvent]("firstFail").where(_.eventType == "fail") //第一次登陆
      .times(3) //宽松近邻
      //.next("secondFail").where(_.eventType == "fail") //第二次登陆
      .within(Time.seconds(5)) //在2秒之内检测匹配

    //2.在分组之后的数据流上应用模式,得到一个PatternStream
    val patternStream: PatternStream[LoginEvent] = CEP.pattern(loginEventStream.keyBy(_.userId), loginFailPattern)

    //3.将检测到的事件序列,转换输出报警信息
    val loginFailStream: DataStream[LoginWarning] = patternStream.select(new LoginFailDetect())

    //打印输出
    loginFailStream.print()

    env.execute("login fail with CEP job")

  }
}

//自定义patternSelectFunction,用来将检测到的连续登陆失败事件,包装成报警信息输出
class LoginFailDetect() extends PatternSelectFunction[LoginEvent, LoginWarning] {
  override def select(map: util.Map[String, util.List[LoginEvent]]): LoginWarning = {
    //map里存放的就是匹配到的一组事件,key是定义好的事件模式名称
    val firstLoginFail = map.get("firstFail").get(0)
    //val secondLoginFail = map.get("secondFail").get(0)
    val lastLoginFail = map.get("firstFail").get(2)
    LoginWarning(firstLoginFail.userId, firstLoginFail.eventTime, lastLoginFail.eventTime, "login fail")
  }
}