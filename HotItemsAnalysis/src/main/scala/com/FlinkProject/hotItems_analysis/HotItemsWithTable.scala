package com.FlinkProject.hotItems_analysis

import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Slide, Table}
import org.apache.flink.table.api.scala._

/** 要求:使用table API进行
  * 需求:每5分钟统计1小时内,热门商品PV前十名
  */

object HotItemsWithTable {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val inputStream = env.readTextFile("D:\\IntelliJ IDEA 2018.2.4\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
    val dataStream: DataStream[UserBehavior] = inputStream.map(line => {
      val str: Array[String] = line.split(",")
      UserBehavior(str(0).toLong, str(1).toLong, str(2).toInt, str(3), str(4).toLong)
    }).assignAscendingTimestamps(_.timestamp * 1000L)
    //创建table环境,要调用table API,创建表的执行环境
    val settings: EnvironmentSettings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      .useBlinkPlanner()
      .build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)
    //将DataStream转换成表,提取需要的字段,进行处理
    val itemTable: Table = tableEnv.fromDataStream(dataStream, 'itemId, 'behavior, 'timestamp.rowtime as 'ts)
    //分组开窗增量聚合
    val countTable: Table = itemTable
      .filter('behavior === "pv")
      .window(Slide over 60.minutes every 5.minutes on 'ts as 'w)
      .groupBy('w, 'itemId)
      .select('itemId, 'itemId.count as 'cnt, 'w.end as 'windowEnd)
    //用SQL实现分组选取topN 的功能
    tableEnv.createTemporaryView("agg", countTable, 'itemId, 'cnt, 'windowEnd)

    val resultTable: Table = tableEnv.sqlQuery(
      """
        |select
        |*
        |from (
        |select *,row_number()over(partition by windowEnd order by cnt desc) as row_num
        |from agg )
        |where row_num <= 5
      """.stripMargin)

    tableEnv.toRetractStream[(Long, Long, Timestamp, Long)](resultTable).print()

    env.execute(" hot items top 5 job")

  }
}
