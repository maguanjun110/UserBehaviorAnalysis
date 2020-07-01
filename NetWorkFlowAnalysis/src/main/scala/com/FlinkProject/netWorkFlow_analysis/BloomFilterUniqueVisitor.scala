

import org.apache.flink.configuration.Configuration
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis


object BloomFilterUniqueVisitor {
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
      .map(data => {
        ("uv", data.userId)
      })
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .trigger(new MyTrigger())
      .process(new UvCountResultWithBloomFilter())

    uvStream.print("uv Count")

    env.execute("Tumbling window uv count job")
  }
}

//自定义触发器,每来一条数据就触发一次窗口计算TriggerResult.(是否触发一次操作,是否清空一次窗口状态)
class MyTrigger() extends Trigger[(String, Long), TimeWindow] {
  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    //数据来了之后,触发计算并清空状态,不保存数据 虽然后面跟得是全量函数,但为了数据的延迟性,目的是:实现增量
    TriggerResult.FIRE_AND_PURGE
  }

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  //fire 触发窗口(对窗口做一次计算) purge 清空窗口状态 CONTINUE FIRE_AND_PURGE(平时如果没有定义延迟时间allowedLateness) FIRE PURGE
  //根据当前的元素或者时间决定是否触发窗口或者清理窗口
  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}
}

//自定义ProcessWindowFunction,把当前数据进行处理,位图保存在redis中
class UvCountResultWithBloomFilter() extends ProcessWindowFunction[(String, Long), UVCount, String, TimeWindow] {

  //建立redis连接
  var jedis: Jedis = _
  var bloom: Bloom = _

  override def open(parameters: Configuration): Unit = {
    jedis = new Jedis("hadoop101", 6379)
    //假设有个窗口1亿数据量去重 10^8,假设一个userId 100byte   1byte = 8bit
    //那么需要10^8word*100Byte/word*8bit/Byte =8 * 10^10 bit
    //位图大小 本来需要的存储空间:10^10 B = 10GB  现在用bloom过滤器,就需要10^8bit存储即100Mbit约等于12MB
    //压缩比是1000的关系,为避免hash碰撞,需要扩充10倍,一个BitMap = 120MB
    //128M*10^6*2^3=10^9=10亿
    //10^9转换为2的多少次幂 2^30 ---> 1 << 30
    bloom = new Bloom(1 << 30) //要处理多大数据决定了布隆过滤器的size
  }
//redis这种效率可能比内存处理的低,但是能够处理大量数据的情况
  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UVCount]): Unit = {
    //每来一个数据,主要是用布隆过滤器判断redis位图中对应位置是否为1
    //位图bitmap用当前窗口的end作为key 保存到redis中(windowEnd, bitmap)
    val storedKey = context.window.getEnd.toString
    //我们把每个窗口的uv count值,作为状态也存入redis中,存成一张叫countMap的表 (windowEnd,count)
    val countMap = "countMap"
    //获取当前的count值
    var count = 0L
    if (jedis.hget(countMap, storedKey) != null) {
      count = jedis.hget(countMap, storedKey).toLong
    }
    //取userId,,计算hash值,判断是够在位图中
    val userId = elements.last._2.toString
    val hashoffset = bloom.hash(userId, 61)
    //jedis中封装了位图判断方法
    val isExist = jedis.getbit(storedKey, hashoffset)
    //如果不存在将对应位图位置设为1,并且count加1;如果位图对应位置存在,不做处理
    if (!isExist) {
      jedis.setbit(storedKey, hashoffset, true)
      jedis.hset(countMap, storedKey, (count + 1).toString)
    }
  }
}

//自定义一个布隆过滤器
class Bloom(size: Long) extends Serializable {
  //定义位图的大小,应该是2的整次幂
  private val cap = size

  //实现一个hash函数,seed 随机数
  def hash(str: String, seed: Int): Long = {
    var result = 0
    for (i <- 0 until str.length) {
      //"ab" seed=3  result1 = 0*3 +97  result2 = 97*3+98
      //"ba" seed=3 result1=0*3 +98  result2 = 98*3 + 97
      result = result * seed + str.charAt(i)
    }
    //对要处理的数据Str处理后,最后result返回一个数值,为了返回一个在cap范围内的一个值 位计算
    (cap - 1) & result
  }


}