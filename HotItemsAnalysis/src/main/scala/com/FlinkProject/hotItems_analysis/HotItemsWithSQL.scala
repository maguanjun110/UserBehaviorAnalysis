package com.FlinkProject.hotItems_analysis

import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._

object HotItemsWithSQL {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置参数:并行度,时间语义
    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    streamEnv.setParallelism(1)
    //加载数据源
    val inputStream: DataStream[String] = streamEnv.readTextFile("D:\\IntelliJ IDEA 2018.2.4\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")

    //将数据源加载为样例类
    val dataStream: DataStream[UserBehavior] = inputStream.map(line => {
      val str: Array[String] = line.split(",")
      UserBehavior(str(0).toLong, str(1).toLong, str(2).toInt, str(3), str(4).toLong)
    }).assignAscendingTimestamps(_.timestamp * 1000L)
    //注册tableenv环境
    val settings: EnvironmentSettings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      .useBlinkPlanner()
      .build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(streamEnv, settings)

    //将流DataStream注册为表
    tableEnv.createTemporaryView("itemsTable", dataStream, 'itemId, 'behavior, 'timestamp.rowtime as 'ts)

    //直接进行SQL查询
    val resultTable = tableEnv.sqlQuery(
      """select
        |*
        |from (
        |select
        |itemId,cnt,windowEnd,row_number()over(partition by windowEnd order by cnt desc) as row_num
        |from(
        |select
        |itemId,
        |count(itemId)as cnt,
        |hop_end(ts,interval '1' hour ,interval '5' minute) as windowEnd
        |from itemsTable
        |group by hop(ts,interval '1' hour ,interval '5' minute),itemId
        |))where row_num <= 5
      """.stripMargin)

    tableEnv.toRetractStream[(Long, Long, Timestamp, Long)](resultTable).print()

    streamEnv.execute("hot items top 5 job with SQL")

  }

}
