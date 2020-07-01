
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

//UV 对userId去重

case class UVCount(windowEnd: Long, count: Long)

object UniqueVisitor {
  def main(args: Array[String]): Unit = {

    //环境创建
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置时间语义
    env.setParallelism(1)
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

    //分配key,包装为二元组开窗聚合:不考虑数据倾斜问题
    val uvStream: DataStream[UVCount] = dataStream
      .filter(_.behavior == "pv")
      .timeWindowAll(Time.hours(1)) //基于datastream开一小时的滚动窗口
      // .apply(new UvCountResult()) //全量窗口函数,拿到窗口的所有数据:延迟性1小时,流式处理应该增量计算
      //需要注意:增量窗口函数和全量窗口函数的区别不是窗口的类型,而是方法的选择apply还是reduce/aggregate
      .aggregate(new UvCountAgg(), new UvCountWindow())
    uvStream.print("uv Count")

    env.execute("Tumbling window uv count job")
  }
}

class UvCountResult() extends AllWindowFunction[UserBehavior, UVCount, TimeWindow] {
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UVCount]): Unit = {
    //定义一个Set集合来保存所有的userId,可以做到自动去重
    var idSet = Set[Long]()
    //将当前窗口的所有数据,添加到set里
    for (userBehavior <- input) {
      idSet += userBehavior.userId
    }
    //输出的set大小,就是去重之后的UV值
    out.collect(UVCount(window.getEnd, idSet.size))
  }
}

class UvCountAgg() extends AggregateFunction[UserBehavior, Set[Long], Long] {//增量去重
  override def createAccumulator(): Set[Long] = Set[Long]()

  override def add(value: UserBehavior, accumulator: Set[Long]): Set[Long] = accumulator + value.userId

  override def getResult(accumulator: Set[Long]): Long = accumulator.size

  override def merge(a: Set[Long], b: Set[Long]): Set[Long] = a ++ b //set合并
}

class UvCountWindow() extends AllWindowFunction[Long, UVCount, TimeWindow] {
  override def apply(window: TimeWindow, input: Iterable[Long], out: Collector[UVCount]): Unit = {
    out.collect(UVCount(window.getEnd, input.head))
  }
}
