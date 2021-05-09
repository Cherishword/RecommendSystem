package ezr.bigdata.recommend.BaseData

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.joda.time.DateTime

object Base_item_count {
  def main(args: Array[String]):Unit= {
    val brandid: String = args(0)
    val daytime= args(1)
    //环境
    val spark = SparkSession
      .builder()
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .appName("base_item_count pro "+brandid)
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode","nonstrict")
//      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
//    每天跑的
    ItemData_insert_dt(spark,brandid,daytime)
    spark.stop()
  }
  def ItemData_insert_dt(spark:SparkSession,brandid:String,daytime:String): Unit ={
    val sql_behavior ="select item_id,behavior_time,sum(ExposeCount) ExposeCount,sum(TriggerCount) TriggerCount,sum(BrowseCount) BrowseCount,sum(CartCount) CartCount,sum(BuyCount) BuyCount " +
      "from recommend_pro.base_behavior_count  where brandid = " + brandid +" and dt=regexp_replace('"+daytime+"', '-', '') group by item_id,behavior_time"
    val behavior = spark.sql(sql_behavior)
    println(sql_behavior)
    behavior.show()
    //写入hive
        val sql1 =  "alter table recommend_pro.base_item_count drop IF EXISTS partition(brandid = "+brandid+",dt="+daytime.replace("-","")+")"
    behavior.repartition(2).persist().createOrReplaceTempView("dt_item_static")
    val sql2 = "insert into table recommend_pro.base_item_count partition(brandid ="+brandid+",dt="+daytime.replace("-","")+") " +
      "select item_id,ExposeCount,TriggerCount,BrowseCount,CartCount,BuyCount from dt_item_static"
    spark.sql(sql1)
    spark.sql("select item_id,ExposeCount,TriggerCount,BrowseCount,CartCount,BuyCount from dt_item_static").show()
    println(sql2)
    spark.sql(sql2)

    val beforeHundredDay = new DateTime(daytime).minusDays(100).toString("yyyMMdd")
    spark.sql(s"alter table recommend_pro.base_item_count drop if exists partition(brandid = $brandid,dt=$beforeHundredDay)")

  }
}
